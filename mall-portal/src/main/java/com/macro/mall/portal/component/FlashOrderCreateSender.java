package com.macro.mall.portal.component;

import com.macro.mall.portal.domain.QueueEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 秒杀异步建单消息发送者（mall1.flash.create） */
@Component
public class FlashOrderCreateSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCreateSender.class);
    @Autowired
    private AmqpTemplate amqpTemplate;

    /** 发送建单消息（消息体为 recordId） */
    public boolean sendMessage(Long recordId) {
        try {
            amqpTemplate.convertAndSend(QueueEnum.QUEUE_FLASH_ORDER_CREATE.getExchange(),
                    QueueEnum.QUEUE_FLASH_ORDER_CREATE.getRouteKey(), recordId);
            LOGGER.info("send flash create recordId:{}", recordId);
            return true;
        } catch (Exception e) {
            LOGGER.error("发送秒杀建单消息失败，recordId:{}，将由兜底扫描任务重投", recordId, e);
            return false;
        }
    }
}