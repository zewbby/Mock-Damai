package com.zewbby.smartticket.service.impl;

import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.service.AsyncOrderMessagePublisher;
import com.zewbby.smartticket.service.LocalMessageService;
import com.zewbby.smartticket.task.LocalMessagePublishTask;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@ConditionalOnProperty(prefix = "smart-ticket.async-order-submit", name = "publisher-mode", havingValue = "outbox", matchIfMissing = true)
public class LocalMessageAsyncOrderMessagePublisher implements AsyncOrderMessagePublisher {

    private final LocalMessageService localMessageService;

    private final LocalMessagePublishTask localMessagePublishTask;

    public LocalMessageAsyncOrderMessagePublisher(LocalMessageService localMessageService,
                                                  LocalMessagePublishTask localMessagePublishTask) {
        this.localMessageService = localMessageService;
        this.localMessagePublishTask = localMessagePublishTask;
    }

    /**
     * 通过 local_message Outbox 链路提交异步下单消息。
     *
     * 这里不直接调用 KafkaTemplate，而是只写本地消息表。LocalMessagePublishTask 的发送器和发送确认超时扫描
     * 负责后续投递状态机。订单超时关闭扫描是另一条独立兜底链路，不属于这条 Outbox 发送状态机。
     * 这样订单服务只表达“我需要发布异步下单消息”，不依赖具体发送细节。
     */
    @Override
    public String publish(AsyncCreateOrderMessage message) {
        String messageId = localMessageService.createAsyncCreateOrderMessage(message);
        publishAfterCommit(messageId);
        return messageId;
    }

    @Override
    public String publish(String messageId, AsyncCreateOrderMessage message) {
        String savedMessageId = localMessageService.createAsyncCreateOrderMessage(messageId, message);
        publishAfterCommit(savedMessageId);
        return savedMessageId;
    }

    private void publishAfterCommit(String messageId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            localMessagePublishTask.publishByMessageId(messageId);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                localMessagePublishTask.publishByMessageId(messageId);
            }
        });
    }
}
