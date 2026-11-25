package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 秒杀订单取消消息消费者，走独立的 mall1.flash.cancel 队列 */
@Component
@RabbitListener(queues = "mall1.flash.cancel")
public class FlashOrderCancelReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCancelReceiver.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    @RabbitHandler
    public void handle(Long orderId) {
        boolean handled = flashPromotionSeckillService.handleFlashOrderCancel(orderId);
        LOGGER.info("process flash orderId:{}, handled:{}", orderId, handled);
    }
}