package com.zewbby.smartticket.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.aop.AdminAudit;
import com.zewbby.smartticket.common.BusinessException;
import com.zewbby.smartticket.config.AsyncOrderSubmitProperties;
import com.zewbby.smartticket.config.LocalMessageProperties;
import com.zewbby.smartticket.config.OrderTimeoutProperties;
import com.zewbby.smartticket.domain.entity.LocalMessage;
import com.zewbby.smartticket.enums.AdminOperationTypeEnum;
import com.zewbby.smartticket.enums.LocalMessageBusinessTypeEnum;
import com.zewbby.smartticket.enums.LocalMessageStatusEnum;
import com.zewbby.smartticket.mapper.LocalMessageMapper;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.mq.OrderTimeoutMessage;
import com.zewbby.smartticket.service.AsyncOrderPartitionService;
import com.zewbby.smartticket.service.LocalMessageService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class LocalMessageServiceImpl implements LocalMessageService {

    private static final int LAST_ERROR_MAX_LENGTH = 512;

    private final LocalMessageMapper localMessageMapper;

    private final ObjectMapper objectMapper;

    private final LocalMessageProperties localMessageProperties;

    private final AsyncOrderSubmitProperties asyncOrderSubmitProperties;

    private final OrderTimeoutProperties orderTimeoutProperties;

    private final AsyncOrderPartitionService asyncOrderPartitionService;

    public LocalMessageServiceImpl(LocalMessageMapper localMessageMapper,
                                   ObjectMapper objectMapper,
                                   LocalMessageProperties localMessageProperties,
                                   AsyncOrderSubmitProperties asyncOrderSubmitProperties,
                                   OrderTimeoutProperties orderTimeoutProperties,
                                   AsyncOrderPartitionService asyncOrderPartitionService) {
        this.localMessageMapper = localMessageMapper;
        this.objectMapper = objectMapper;
        this.localMessageProperties = localMessageProperties;
        this.asyncOrderSubmitProperties = asyncOrderSubmitProperties;
        this.orderTimeoutProperties = orderTimeoutProperties;
        this.asyncOrderPartitionService = asyncOrderPartitionService;
    }

    /**
     * 创建异步下单本地消息。
     *
     * 业务数据写成功后，如果应用马上调用 Kafka 发送，可能出现“数据库事务已提交，但发送 MQ 失败或进程宕机”的不一致。
     * Outbox 模式把“待发送消息”也写进本地数据库，让业务状态和消息意图尽量处在同一个事务里：
     * 订单请求进入 QUEUED 前，先落一条 INIT 消息。即使应用随后宕机，发送器也能从 local_message 找回这条待发送消息。
     *
     * @param message 异步创建订单消息，requestId 是业务主键。
     * @return local_message.message_id，用于后续 Kafka send callback 状态回写和人工排查。
     */
    @Override
    public String createAsyncCreateOrderMessage(AsyncCreateOrderMessage message) {
        return createAsyncCreateOrderMessage(asyncCreateOrderMessageId(message.getRequestId()), message);
    }

    @Override
    public String createAsyncCreateOrderMessage(String messageId, AsyncCreateOrderMessage message) {
        return createLocalMessage(
                messageId,
                LocalMessageBusinessTypeEnum.ASYNC_CREATE_ORDER.getCode(),
                message.getRequestId(),
                asyncOrderSubmitProperties.getKafkaAsyncCreateOrderTopic(),
                asyncOrderPartitionService.partitionKey(message),
                message
        );
    }

    /**
     * 创建订单超时关闭本地消息。
     *
     * 订单创建成功后如果直接发送超时事件，一旦数据库事务提交后应用宕机、Kafka 短暂不可用，
     * 就会出现“订单存在但没有超时关闭消息”的风险，未支付订单会长期占住 locked_stock。
     * 因此超时关闭消息也必须纳入 Outbox：先和订单状态一起落 local_message，再由发送器统一投递并等待 Kafka send callback。
     */
    @Override
    public String createOrderTimeoutCloseMessage(OrderTimeoutMessage message) {
        String messageId = createLocalMessage(
                generateMessageId(),
                LocalMessageBusinessTypeEnum.ORDER_TIMEOUT_CLOSE.getCode(),
                String.valueOf(message.getOrderId()),
                orderTimeoutProperties.getKafkaOrderTimeoutTopic(),
                orderTimeoutKey(message),
                message
        );
        message.setMessageId(messageId);
        return messageId;
    }

    @Override
    public String createDomainEventMessage(String businessType,
                                           String businessKey,
                                           String topic,
                                           String routingKey,
                                           Object payload) {
        return createLocalMessage(
                generateMessageId(),
                businessType,
                businessKey,
                topic,
                routingKey,
                payload
        );
    }

    private String createLocalMessage(String messageId,
                                      String businessType,
                                      String businessKey,
                                      String topic,
                                      String partitionKey,
                                      Object payload) {
        LocalDateTime now = LocalDateTime.now();
        LocalMessage localMessage = new LocalMessage();
        localMessage.setMessageId(messageId);
        localMessage.setBusinessType(businessType);
        localMessage.setBusinessKey(businessKey);
        // 数据库沿用历史 exchange_name / routing_key 列名；当前 sender 分别按 Kafka topic / record key 解释。
        localMessage.setExchangeName(topic);
        localMessage.setRoutingKey(partitionKey);
        localMessage.setPayload(toJson(payload));
        localMessage.setStatus(LocalMessageStatusEnum.INIT.getCode());
        localMessage.setRetryCount(0);
        localMessage.setMaxRetryCount(localMessageProperties.getDefaultMaxRetryCount());
        localMessage.setNextRetryTime(null);
        localMessage.setLastError(null);
        localMessage.setSentAt(null);
        localMessage.setConfirmedAt(null);
        localMessage.setDeadAt(null);
        localMessage.setCreatedAt(now);
        localMessage.setUpdatedAt(now);

        int rows = localMessageMapper.insert(localMessage);
        if (rows != 1) {
            throw new BusinessException("本地消息创建失败");
        }
        return localMessage.getMessageId();
    }

    private String asyncCreateOrderMessageId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return generateMessageId();
        }
        return "MSG" + requestId;
    }

    /**
     * 以数据库行锁批量领取待投递消息。多实例发送器会跳过其他实例已锁定的行，
     * 将原本每条消息一次条件更新的竞争收敛为一次短事务和一次批量更新。
     */
    @Override
    @Transactional
    public List<LocalMessage> claimPublishableMessages(LocalDateTime now, Integer limit) {
        List<LocalMessage> messages = localMessageMapper.selectPublishableMessagesForUpdate(now, limit);
        if (messages == null || messages.isEmpty()) {
            return List.of();
        }
        List<Long> ids = messages.stream()
                .map(LocalMessage::getId)
                .toList();
        if (ids.stream().anyMatch(id -> id == null)) {
            throw new BusinessException("待发送本地消息缺少主键");
        }
        int updatedRows = localMessageMapper.markSendingBatch(ids);
        if (updatedRows != messages.size()) {
            throw new BusinessException("批量领取本地消息失败");
        }
        messages.forEach(message -> message.setStatus(LocalMessageStatusEnum.SENDING.getCode()));
        return messages;
    }

    /**
     * 抢占一条待发送消息。
     *
     * 发送器可能多线程、多实例部署。如果只是 Java 先查出 INIT/FAILED 再 if 判断，两个实例可能同时看到同一条消息可发。
     * 这里用 SQL 条件更新作为并发开关：只有影响行数为 1 的线程拿到发送权，其他线程必须跳过。
     */
    @Override
    public boolean tryMarkSending(LocalMessage message) {
        return localMessageMapper.tryMarkSending(message.getId()) == 1;
    }

    @Override
    public void markSent(Long id) {
        localMessageMapper.markSent(id, LocalDateTime.now());
    }

    @Override
    public void markConfirmed(String messageId) {
        localMessageMapper.markConfirmed(messageId, LocalDateTime.now());
    }

    @Override
    public void markPublishFailed(LocalMessage message, String reason) {
        LocalDateTime now = LocalDateTime.now();
        int nextRetryCount = message.getRetryCount() + 1;
        LocalDateTime deadAt = nextRetryCount >= message.getMaxRetryCount() ? now : null;
        LocalDateTime nextRetryTime = deadAt == null ? calculateNextRetryTime(now, nextRetryCount) : null;
        localMessageMapper.markPublishFailedById(
                message.getId(),
                trimLastError(reason),
                nextRetryTime,
                deadAt
        );
    }

    @Override
    public List<LocalMessage> selectConfirmTimeoutMessages(LocalDateTime timeoutBefore, Integer limit) {
        return localMessageMapper.selectConfirmTimeoutMessages(timeoutBefore, limit);
    }

    @Override
    public List<LocalMessage> selectRecent(String status, Integer limit) {
        return localMessageMapper.selectRecent(status, limit);
    }

    @Override
    public long countByStatus(String status) {
        Long count = localMessageMapper.countByStatus(status);
        return count == null ? 0L : count;
    }

    @Override
    public LocalMessage getByMessageId(String messageId) {
        return localMessageMapper.selectByMessageId(messageId);
    }

    @Override
    @AdminAudit(operation = AdminOperationTypeEnum.LOCAL_MESSAGE_RETRY, resourceType = "LOCAL_MESSAGE", resourceId = "#p0")
    public void retryManually(String messageId) {
        int rows = localMessageMapper.resetForManualRetry(messageId);
        if (rows != 1) {
            throw new BusinessException("消息不存在或当前状态不允许重试");
        }
    }

    @Override
    @AdminAudit(operation = AdminOperationTypeEnum.LOCAL_MESSAGE_MARK_DEAD, resourceType = "LOCAL_MESSAGE", resourceId = "#p0")
    public void markDeadManually(String messageId) {
        int rows = localMessageMapper.markDeadByMessageId(messageId, "人工标记为DEAD", LocalDateTime.now());
        if (rows != 1) {
            throw new BusinessException("消息不存在或当前状态不允许标记DEAD");
        }
    }

    private LocalDateTime calculateNextRetryTime(LocalDateTime now, int nextRetryCount) {
        if (nextRetryCount <= 1) {
            return now.plusSeconds(10);
        }
        if (nextRetryCount == 2) {
            return now.plusSeconds(30);
        }
        return now.plusSeconds(60);
    }

    private String toJson(Object message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new BusinessException("本地消息序列化失败");
        }
    }

    private String generateMessageId() {
        return "MSG" + UUID.randomUUID().toString().replace("-", "");
    }

    private String orderTimeoutKey(OrderTimeoutMessage message) {
        if (message == null || message.getOrderId() == null) {
            return "order:unknown";
        }
        return "order:" + message.getOrderId();
    }

    private String trimLastError(String message) {
        if (message == null) {
            return "消息发送失败";
        }
        if (message.length() <= LAST_ERROR_MAX_LENGTH) {
            return message;
        }
        return message.substring(0, LAST_ERROR_MAX_LENGTH);
    }
}
