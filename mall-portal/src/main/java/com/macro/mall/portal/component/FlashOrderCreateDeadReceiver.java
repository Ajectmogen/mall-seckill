package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 秒杀异步建单死信消费者 */
@Component
@RabbitListener(queues = "mall1.flash.dead")
public class FlashOrderCreateDeadReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCreateDeadReceiver.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    @RabbitHandler
    public void handle(Long recordId) {
        boolean handled = flashPromotionSeckillService.handleFlashOrderCreateDead(recordId);
        LOGGER.warn("process flash dead recordId:{}, handled:{}", recordId, handled);
    }
}