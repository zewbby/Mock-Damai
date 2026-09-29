package com.zewbby.smartticket.service;

import com.zewbby.smartticket.domain.entity.LocalMessage;
import com.zewbby.smartticket.mq.AsyncCreateOrderMessage;
import com.zewbby.smartticket.mq.OrderTimeoutMessage;

import java.time.LocalDateTime;
import java.util.List;

public interface LocalMessageService {

    String createAsyncCreateOrderMessage(AsyncCreateOrderMessage message);

    String createAsyncCreateOrderMessage(String messageId, AsyncCreateOrderMessage message);

    String createOrderTimeoutCloseMessage(OrderTimeoutMessage message);

    String createDomainEventMessage(String businessType,
                                    String businessKey,
                                    String topic,
                                    String routingKey,
                                    Object payload);

    List<LocalMessage> claimPublishableMessages(LocalDateTime now, Integer limit);

    boolean tryMarkSending(LocalMessage message);

    void markSent(Long id);

    void markConfirmed(String messageId);

    void markPublishFailed(LocalMessage message, String reason);

    List<LocalMessage> selectConfirmTimeoutMessages(LocalDateTime timeoutBefore, Integer limit);

    List<LocalMessage> selectRecent(String status, Integer limit);

    long countByStatus(String status);

    LocalMessage getByMessageId(String messageId);

    void retryManually(String messageId);

    void markDeadManually(String messageId);
}
