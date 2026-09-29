package com.zewbby.smartticket.mapper;

import com.zewbby.smartticket.domain.entity.LocalMessage;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface LocalMessageMapper {

    int insert(LocalMessage message);

    LocalMessage selectByMessageId(@Param("messageId") String messageId);

    List<LocalMessage> selectPublishableMessagesForUpdate(@Param("now") LocalDateTime now,
                                                           @Param("limit") Integer limit);

    int markSendingBatch(@Param("ids") List<Long> ids);

    int tryMarkSending(@Param("id") Long id);

    int markSent(@Param("id") Long id, @Param("sentAt") LocalDateTime sentAt);

    int markConfirmed(@Param("messageId") String messageId,
                      @Param("confirmedAt") LocalDateTime confirmedAt);

    int markPublishFailedById(@Param("id") Long id,
                              @Param("lastError") String lastError,
                              @Param("nextRetryTime") LocalDateTime nextRetryTime,
                              @Param("deadAt") LocalDateTime deadAt);

    List<LocalMessage> selectConfirmTimeoutMessages(@Param("timeoutBefore") LocalDateTime timeoutBefore,
                                                    @Param("limit") Integer limit);

    List<LocalMessage> selectRecent(@Param("status") String status, @Param("limit") Integer limit);

    Long countByStatus(@Param("status") String status);

    int resetForManualRetry(@Param("messageId") String messageId);

    int markDeadByMessageId(@Param("messageId") String messageId,
                            @Param("lastError") String lastError,
                            @Param("deadAt") LocalDateTime deadAt);
}
