package com.zewbby.smartticket.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("local_message")
public class LocalMessage {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String messageId;

    private String businessType;

    private String businessKey;

    /**
     * 历史列名保留为 exchange_name；当前 Local Message sender 实际将它作为 Kafka topic 使用。
     */
    private String exchangeName;

    /**
     * 历史列名保留为 routing_key；当前 Local Message sender 实际将它作为 Kafka record key 使用。
     */
    private String routingKey;

    private String payload;

    private String status;

    private Integer retryCount;

    private Integer maxRetryCount;

    private LocalDateTime nextRetryTime;

    private String lastError;

    private LocalDateTime sentAt;

    private LocalDateTime confirmedAt;

    private LocalDateTime deadAt;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
