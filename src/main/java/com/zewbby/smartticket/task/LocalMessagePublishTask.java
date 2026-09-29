package com.zewbby.smartticket.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.aop.MonitoredOperation;
import com.zewbby.smartticket.config.LocalMessageProperties;
import com.zewbby.smartticket.domain.entity.LocalMessage;
import com.zewbby.smartticket.domain.event.OrderCreatedEvent;
import com.zewbby.smartticket.domain.event.PaymentPaidEvent;
import com.zewbby.smartticket.domain.event.StockChangedEvent;
import com.zewbby.smartticket.enums.LocalMessageBusinessTypeEnum;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.mq.OrderTimeoutMessage;
import com.zewbby.smartticket.service.LocalMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class LocalMessagePublishTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalMessagePublishTask.class);

    private final LocalMessageService localMessageService;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper;

    private final LocalMessageProperties localMessageProperties;

    public LocalMessagePublishTask(LocalMessageService localMessageService,
                                   KafkaTemplate<String, Object> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   LocalMessageProperties localMessageProperties) {
        this.localMessageService = localMessageService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.localMessageProperties = localMessageProperties;
    }

    /**
     * 扫描 INIT / FAILED 消息并投递到 Kafka。
     *
     * INIT 表示业务事务已经提交了“必须发送这条消息”的意图；FAILED 表示上一次发送调用、send callback
     * 或发送确认超时后可以重试。retry_count、max_retry_count、next_retry_time 控制重试次数和退避时间，
     * 避免 Kafka 不可用时无限打爆数据库和 MQ。
     *
     * 多实例部署时，发送器通过 FOR UPDATE SKIP LOCKED 跳过其他实例已锁定的行，
     * 再把当前批次一次性更新为 SENDING；因此同一消息只会由一个实例领取。
     */
    @Scheduled(fixedDelayString = "#{@localMessageProperties.publishFixedDelayMillis}")
    @MonitoredOperation(value = "local_message.publish_pending", slowThresholdMs = 1000L)
    public void publishPendingMessages() {
        // 开关校验
        if (!localMessageProperties.isSenderEnabled()) {
            return;
        }
        // 在一个短事务中锁定并领取待投递消息，避免多个实例重复扫描后逐条竞争。
        List<LocalMessage> messages = localMessageService.claimPublishableMessages(
                LocalDateTime.now(),
                localMessageProperties.getBatchSize()
        );
        for (LocalMessage message : messages) {
            // 消息已由批量领取操作原子地标记为 SENDING，直接异步发送。
            publishOne(message);
        }
    }

    /**
     * 由业务线程在事务提交后（afterCommit）触发一次即时发送，避免正常路径必须等待下一轮周期扫描。
     * 周期扫描仍负责应用宕机、即时发送失败等情况下残留的 INIT / FAILED 消息，因此即时路径不是可靠性的唯一边界。
     *
     * @param messageId local_message.message_id
     */
    @MonitoredOperation(value = "local_message.publish_by_message_id", slowThresholdMs = 500L)
    public void publishByMessageId(String messageId) {
        if (!localMessageProperties.isSenderEnabled()) {
            return;
        }
        LocalMessage message = localMessageService.getByMessageId(messageId);
        if (message == null) {
            LOGGER.warn("Skipped immediate local message publish because message does not exist, messageId={}",
                    messageId);
            return;
        }
        //状态抢占，确保安全性
        if (!localMessageService.tryMarkSending(message)) {
            LOGGER.debug("Skipped immediate local message publish because message is not claimable, messageId={}",
                    messageId);
            return;
        }
        publishOne(message);
    }

    /**
     * 发送一条本地消息。
     *
     * KafkaTemplate.send 返回不抛异常，只能说明客户端发送调用没有立刻失败；真正的发送结果由返回 Future 的 callback 决定。
     * 为了降低 Outbox 写放大，默认不再强制写 SENT 中间态；消息保持 SENDING，直到 send callback 成功后直接改 CONFIRMED。
     * 发送确认超时扫描覆盖 SENDING / SENT 两种状态，所以发送线程退出、callback 丢失或应用重启仍能重新置 FAILED 后重试。
     * 对订单超时关闭也是同一规则：CONFIRMED 只代表 Kafka 已确认接收该消息，不代表订单已经被关闭；
     * 真正关闭成功与否必须看超时消费者读取数据库订单状态后的幂等处理结果。
     */

    /**
     * 调用 Spring Kafka 发送消息。
     * 架构中的作用：将 local_message 中记录的 topic、partition key 与 payload 投递到 Kafka。
     *
     * @param localMessage
     */
    void publishOne(LocalMessage localMessage) {
        try {
            Object payload = toPayload(localMessage);
            kafkaTemplate.send(
                    localMessage.getExchangeName(),
                    localMessage.getRoutingKey(),
                    payload
            ).whenComplete((result, exception) -> {
                if (exception == null) {
                    localMessageService.markConfirmed(localMessage.getMessageId());
                    LOGGER.debug("Kafka confirmed local message, messageId={}, businessKey={}",
                            localMessage.getMessageId(), localMessage.getBusinessKey());
                    return;
                }
                localMessageService.markPublishFailed(localMessage, exception.getMessage());
                LOGGER.warn("Kafka failed to send local message, messageId={}, businessKey={}",
                        localMessage.getMessageId(), localMessage.getBusinessKey(), exception);
            });
            if (localMessageProperties.isMarkSentEnabled()) {
                localMessageService.markSent(localMessage.getId());
            }
            LOGGER.debug("Submitted local message to Kafka, messageId={}, businessKey={}",
                    localMessage.getMessageId(), localMessage.getBusinessKey());
        } catch (Exception exception) {
            localMessageService.markPublishFailed(localMessage, exception.getMessage());
            LOGGER.warn("Failed to send local message, messageId={}, businessKey={}",
                    localMessage.getMessageId(), localMessage.getBusinessKey(), exception);
        }
    }

    /**
     * 扫描长时间没有 Confirm 的消息。
     *
     * 消息可能卡在 SENDING/SENT：例如发送线程宕机、Kafka callback 没执行、回调 DB 更新失败，或者应用在回调前退出。
     * 不能让这些消息永久停留在“等待确认”，所以使用 updated_at 做超时判断，转回 FAILED 等待重试。
     * schema 中对应索引是 idx_status_updated_at(status, updated_at)。
     */
    @Scheduled(fixedDelayString = "#{@localMessageProperties.confirmTimeoutScanFixedDelayMillis}")
    @MonitoredOperation(value = "local_message.scan_confirm_timeout", slowThresholdMs = 1000L)
    public void scanConfirmTimeoutMessages() {
        //开关校验
        if (!localMessageProperties.isSenderEnabled()) {
            return;
        }
        //计算超时截止线
        LocalDateTime timeoutBefore = LocalDateTime.now()
                .minusSeconds(localMessageProperties.getConfirmTimeoutSeconds());

        //查找出batchSize规模的超时message
        List<LocalMessage> messages = localMessageService.selectConfirmTimeoutMessages(
                timeoutBefore,
                localMessageProperties.getBatchSize()
        );
        //把这一批超时message全部标记失败
        for (LocalMessage message : messages) {
            localMessageService.markPublishFailed(message, "Kafka发送确认超时");
        }
    }

    /**
     * 反序列化工具：发送时，需要根据业务类型（businessType），用 Jackson 的 objectMapper 把它还原成真正的 Java 对象（秒杀创单对象 或 超时关单对象）。
     * @param localMessage
     * @return
     * @throws Exception
     */
    private Object toPayload(LocalMessage localMessage) throws Exception {
        if (LocalMessageBusinessTypeEnum.ASYNC_CREATE_ORDER.getCode().equals(localMessage.getBusinessType())) {
            return objectMapper.readValue(localMessage.getPayload(), AsyncCreateOrderMessage.class);
        }
        if (LocalMessageBusinessTypeEnum.ORDER_TIMEOUT_CLOSE.getCode().equals(localMessage.getBusinessType())) {
            return objectMapper.readValue(localMessage.getPayload(), OrderTimeoutMessage.class);
        }
        if (LocalMessageBusinessTypeEnum.ORDER_CREATED_EVENT.getCode().equals(localMessage.getBusinessType())) {
            return objectMapper.readValue(localMessage.getPayload(), OrderCreatedEvent.class);
        }
        if (LocalMessageBusinessTypeEnum.PAYMENT_PAID_EVENT.getCode().equals(localMessage.getBusinessType())) {
            return objectMapper.readValue(localMessage.getPayload(), PaymentPaidEvent.class);
        }
        if (LocalMessageBusinessTypeEnum.STOCK_CHANGED_EVENT.getCode().equals(localMessage.getBusinessType())) {
            return objectMapper.readValue(localMessage.getPayload(), StockChangedEvent.class);
        }
        throw new IllegalArgumentException("不支持的本地消息业务类型：" + localMessage.getBusinessType());
    }
}
