package com.macro.mall.portal.component;

import com.macro.mall.portal.domain.QueueEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 秒杀异步建单消息的发送者（v2）
 * <p>
 * 与「秒杀订单取消」使用完全独立的队列（mall1.flash.create），取消消息的延迟 TTL 不会和建单消息互相阻塞。
 * 发送失败**不抛异常**：此时 sms_flash_promotion_record 已经是 status=0（排队中），
 * 交给 {@link FlashOrderCreateSweeperTask} 按「status=0 且 create_time 超过 15 秒」重投，接口仍正常返回排队中。
 * Created by mall-1 seckill v2.
 */
@Component
public class FlashOrderCreateSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCreateSender.class);
    @Autowired
    private AmqpTemplate amqpTemplate;

    /**
     * 发送建单消息（消息体为 recordId）
     *
     * @return true 表示已交给 RabbitMQ；false 表示发送失败（等待兜底扫描重投）
     */
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