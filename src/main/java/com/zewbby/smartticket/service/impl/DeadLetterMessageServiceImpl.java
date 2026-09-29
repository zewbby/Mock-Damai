package com.zewbby.smartticket.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zewbby.smartticket.aop.AdminAudit;
import com.zewbby.smartticket.common.BusinessException;
import com.zewbby.smartticket.config.AsyncOrderSubmitProperties;
import com.zewbby.smartticket.domain.entity.DeadLetterMessage;
import com.zewbby.smartticket.domain.entity.TicketOrderRequest;
import com.zewbby.smartticket.enums.AdminOperationTypeEnum;
import com.zewbby.smartticket.enums.CompensationStatusEnum;
import com.zewbby.smartticket.enums.ConsumerExceptionTypeEnum;
import com.zewbby.smartticket.enums.DeadLetterStatusEnum;
import com.zewbby.smartticket.enums.LocalMessageBusinessTypeEnum;
import com.zewbby.smartticket.enums.OrderRequestStatusEnum;
import com.zewbby.smartticket.mapper.DeadLetterMessageMapper;
import com.zewbby.smartticket.mapper.OrderRequestMapper;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.service.AsyncOrderMessagePublisher;
import com.zewbby.smartticket.service.DeadLetterMessageService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class DeadLetterMessageServiceImpl implements DeadLetterMessageService {

    private static final int DEFAULT_LIMIT = 50;

    private static final int MAX_LIMIT = 200;

    private static final int DEFAULT_MAX_RETRY_COUNT = 3;

    private static final int EXCEPTION_MESSAGE_MAX_LENGTH = 512;

    private final DeadLetterMessageMapper deadLetterMessageMapper;

    private final OrderRequestMapper orderRequestMapper;

    private final AsyncOrderMessagePublisher asyncOrderMessagePublisher;

    private final ObjectMapper objectMapper;

    private final AsyncOrderSubmitProperties asyncOrderSubmitProperties;

    public DeadLetterMessageServiceImpl(DeadLetterMessageMapper deadLetterMessageMapper,
                                        OrderRequestMapper orderRequestMapper,
                                        AsyncOrderMessagePublisher asyncOrderMessagePublisher,
                                        ObjectMapper objectMapper,
                                        AsyncOrderSubmitProperties asyncOrderSubmitProperties) {
        this.deadLetterMessageMapper = deadLetterMessageMapper;
        this.orderRequestMapper = orderRequestMapper;
        this.asyncOrderMessagePublisher = asyncOrderMessagePublisher;
        this.objectMapper = objectMapper;
        this.asyncOrderSubmitProperties = asyncOrderSubmitProperties;
    }

    /**
     * 将消费失败消息落到 dead_letter_message。
     *
     * Broker DLT 或死信表不是“垃圾桶”，而是异常消息的工作台：我们保留原始 payload、业务主键、来源 channel 和异常分类，
     * 后续才能判断应该 retry、ignore 还是 resolve。只打日志会在高并发下被刷掉；只留在 Broker DLT 又不方便
     * 和 ticket_order_request 的业务状态放在一起排查。
     */
    @Override
    public void recordDeadLetter(String messageId,
                                 String businessType,
                                 String businessKey,
                                 String queueName,
                                 String exchangeName,
                                 String routingKey,
                                 String payload,
                                 ConsumerExceptionTypeEnum exceptionType,
                                 String exceptionMessage) {
        LocalDateTime now = LocalDateTime.now();

        DeadLetterMessage deadLetterMessage = new DeadLetterMessage();
        deadLetterMessage.setMessageId(messageId);
        deadLetterMessage.setBusinessType(businessType);
        deadLetterMessage.setBusinessKey(businessKey == null ? "UNKNOWN" : businessKey);
        deadLetterMessage.setQueueName(queueName == null
                ? asyncOrderSubmitProperties.getKafkaAsyncCreateOrderDeadLetterTopic()
                : queueName);
        deadLetterMessage.setExchangeName(exchangeName);
        deadLetterMessage.setRoutingKey(routingKey);
        deadLetterMessage.setPayload(payload == null ? "{}" : payload);
        deadLetterMessage.setExceptionType(exceptionType.getCode());
        deadLetterMessage.setExceptionMessage(trim(exceptionMessage));
        deadLetterMessage.setStatus(DeadLetterStatusEnum.PENDING.getCode());
        deadLetterMessage.setRetryCount(0);
        deadLetterMessage.setMaxRetryCount(DEFAULT_MAX_RETRY_COUNT);
        deadLetterMessage.setLastRetryAt(null);
        deadLetterMessage.setResolvedAt(null);
        deadLetterMessage.setCreatedAt(now);
        deadLetterMessage.setUpdatedAt(now);
        deadLetterMessageMapper.insert(deadLetterMessage);
    }

    @Override
    public void recordAsyncCreateOrderDeadLetter(AsyncCreateOrderMessage message,
                                                 String queueName,
                                                 String exchangeName,
                                                 String routingKey,
                                                 String messageId,
                                                 ConsumerExceptionTypeEnum exceptionType,
                                                 String exceptionMessage) {
        String businessKey = message == null ? "UNKNOWN" : message.getRequestId();
        String resolvedMessageId = resolveMessageId(message, messageId);
        recordDeadLetter(
                resolvedMessageId,
                LocalMessageBusinessTypeEnum.ASYNC_CREATE_ORDER.getCode(),
                businessKey,
                queueName,
                exchangeName,
                routingKey,
                toJson(message),
                exceptionType,
                exceptionMessage
        );
    }

    @Override
    public List<DeadLetterMessage> selectRecent(String status, Integer limit) {
        return deadLetterMessageMapper.selectRecent(status, normalizeLimit(limit));
    }

    @Override
    public DeadLetterMessage getById(Long id) {
        DeadLetterMessage deadLetterMessage = deadLetterMessageMapper.selectById(id);
        if (deadLetterMessage == null) {
            throw new BusinessException("死信消息不存在");
        }
        return deadLetterMessage;
    }

    /**
     * 人工重试死信消息。
     *
     * 这里不直接调用消费者方法。正确做法是先检查 request 当前业务状态，
     * 再走当前启用的 AsyncOrderMessagePublisher：RocketMQ / Kafka direct 回到对应 Broker，Outbox 则先写 local_message 再投递 Kafka。
     * 如果 request 已 SUCCESS，重试会制造重复订单风险；如果已经 COMPENSATED，说明 Redis 预扣已经释放，
     * 此时直接重投会绕过入口预扣语义，所以当前阶段拒绝重试，留给后续人工补偿流程处理。
     */
    @Override
    @AdminAudit(operation = AdminOperationTypeEnum.DEAD_LETTER_RETRY, resourceType = "DEAD_LETTER_MESSAGE", resourceId = "#p0")
    @Transactional
    public void retry(Long id) {
        DeadLetterMessage deadLetterMessage = getById(id);
        if (!DeadLetterStatusEnum.PENDING.getCode().equals(deadLetterMessage.getStatus())
                && !DeadLetterStatusEnum.FAILED.getCode().equals(deadLetterMessage.getStatus())) {
            throw new BusinessException("当前死信状态不允许重试");
        }
        if (!LocalMessageBusinessTypeEnum.ASYNC_CREATE_ORDER.getCode().equals(deadLetterMessage.getBusinessType())) {
            throw new BusinessException("当前死信业务类型不支持重试");
        }

        AsyncCreateOrderMessage message = fromJson(deadLetterMessage.getPayload());
        TicketOrderRequest request = orderRequestMapper.selectByRequestId(message.getRequestId());
        if (request == null) {
            throw new BusinessException("异步下单请求不存在，不能重试");
        }
        ensureRetryableRequestState(request);

        if (OrderRequestStatusEnum.FAILED.getCode().equals(request.getStatus())
                || OrderRequestStatusEnum.PROCESSING.getCode().equals(request.getStatus())) {
            int resetRows = orderRequestMapper.resetForManualRetry(request.getId());
            if (resetRows != 1) {
                throw new BusinessException("异步下单请求状态不允许重试");
            }
        }

        String newMessageId = asyncOrderMessagePublisher.publish(message);
        int messageRows;
        if (OrderRequestStatusEnum.PRE_DEDUCTED.getCode().equals(request.getStatus())) {
            messageRows = orderRequestMapper.markQueued(request.getId(), newMessageId);
        } else {
            messageRows = orderRequestMapper.refreshQueuedMessage(request.getId(), newMessageId);
        }
        if (messageRows != 1) {
            throw new BusinessException("异步下单请求消息ID更新失败");
        }

        int rows = deadLetterMessageMapper.markRetried(id, LocalDateTime.now());
        if (rows != 1) {
            throw new BusinessException("死信消息状态不允许重试");
        }
    }

    @Override
    @AdminAudit(operation = AdminOperationTypeEnum.DEAD_LETTER_IGNORE, resourceType = "DEAD_LETTER_MESSAGE", resourceId = "#p0")
    public void ignore(Long id) {
        int rows = deadLetterMessageMapper.markIgnored(id, LocalDateTime.now());
        if (rows != 1) {
            throw new BusinessException("死信消息不存在或状态不允许忽略");
        }
    }

    @Override
    @AdminAudit(operation = AdminOperationTypeEnum.DEAD_LETTER_RESOLVE, resourceType = "DEAD_LETTER_MESSAGE", resourceId = "#p0")
    public void resolve(Long id) {
        int rows = deadLetterMessageMapper.markResolved(id, LocalDateTime.now());
        if (rows != 1) {
            throw new BusinessException("死信消息不存在或状态不允许标记解决");
        }
    }

    private void ensureRetryableRequestState(TicketOrderRequest request) {
        if (OrderRequestStatusEnum.SUCCESS.getCode().equals(request.getStatus())) {
            throw new BusinessException("异步下单请求已成功，不能重试");
        }
        if (OrderRequestStatusEnum.COMPENSATED.getCode().equals(request.getStatus())
                || Boolean.TRUE.equals(request.getCompensated())
                || CompensationStatusEnum.COMPENSATED.getCode().equals(request.getCompensationStatus())) {
            throw new BusinessException("Redis 预扣已补偿，不能直接重试");
        }
        if (Boolean.FALSE.equals(request.getRedisDeducted())) {
            throw new BusinessException("请求没有 Redis 预扣记录，不能直接重试");
        }
        if (CompensationStatusEnum.COMPENSATING.getCode().equals(request.getCompensationStatus())) {
            throw new BusinessException("请求正在补偿中，不能重试");
        }
        if (OrderRequestStatusEnum.CANCELLED.getCode().equals(request.getStatus())) {
            throw new BusinessException("异步下单请求已取消，不能重试");
        }
    }

    private String resolveMessageId(AsyncCreateOrderMessage message, String messageId) {
        if (messageId != null && !messageId.isBlank()) {
            return messageId;
        }
        if (message == null || message.getRequestId() == null) {
            return null;
        }
        TicketOrderRequest request = orderRequestMapper.selectByRequestId(message.getRequestId());
        return request == null ? null : request.getMessageId();
    }

    private String toJson(AsyncCreateOrderMessage message) {
        if (message == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new BusinessException("死信消息序列化失败");
        }
    }

    private AsyncCreateOrderMessage fromJson(String payload) {
        try {
            return objectMapper.readValue(payload, AsyncCreateOrderMessage.class);
        } catch (JsonProcessingException exception) {
            throw new BusinessException("死信消息反序列化失败");
        }
    }

    private Integer normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    private String trim(String message) {
        if (message == null || message.isBlank()) {
            return "消费失败";
        }
        if (message.length() <= EXCEPTION_MESSAGE_MAX_LENGTH) {
            return message;
        }
        return message.substring(0, EXCEPTION_MESSAGE_MAX_LENGTH);
    }
}
