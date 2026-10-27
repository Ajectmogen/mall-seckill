package com.macro.mall.portal.component;

import com.macro.mall.portal.domain.QueueEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单取消消息的发送者
 * <p>
 * 与普通订单取消消息（CancelOrderSender）使用完全独立的交换机/队列，
 * 避免秒杀场景的高并发消息把普通订单的取消消息堵在同一个队列里（队头阻塞）。
 * Created by mall-1 seckill v1.
 */
@Component
public class FlashOrderCancelSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCancelSender.class);
    @Autowired
    private AmqpTemplate amqpTemplate;

    /**
     * 发送延迟消息，delayTimes 毫秒后进入秒杀取消队列
     */
    public void sendMessage(Long orderId, final long delayTimes) {
        //给秒杀延迟队列发送消息
        amqpTemplate.convertAndSend(QueueEnum.QUEUE_TTL_FLASH_ORDER_CANCEL.getExchange(), QueueEnum.QUEUE_TTL_FLASH_ORDER_CANCEL.getRouteKey(), orderId, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                //给消息设置延迟毫秒值（秒杀消息统一使用 flash_order_overtime，TTL一致，不会在死信队列内部产生队头阻塞）
                message.getMessageProperties().setExpiration(String.valueOf(delayTimes));
                return message;
            }
        });
        LOGGER.info("send flash orderId:{}", orderId);
    }
}