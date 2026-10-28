package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 秒杀异步建单死信队列的接收者（v2）
 * <p>
 * 建单队列配置了 x-dead-letter-exchange/x-dead-letter-routing-key 指向 mall1.flash.dead：
 * 消费者若把消息拒绝（requeue=false）或消息过期，就会落到这里。这里把记录置 3（建单失败）并归还 Redis 名额，
 * 保证死信不会变成「记录永远排队中」的黑洞。
 * Created by mall-1 seckill v2.
 */
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