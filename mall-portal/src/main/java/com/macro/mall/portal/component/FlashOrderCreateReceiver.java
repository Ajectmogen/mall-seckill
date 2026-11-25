package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** 秒杀异步建单消费者：CAS 领取保证幂等，失败时补偿 Redis 名额 */
@Component
@RabbitListener(queues = "mall1.flash.create")
public class FlashOrderCreateReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCreateReceiver.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    @RabbitHandler
    public void handle(Long recordId) {
        flashPromotionSeckillService.createFlashOrderFromRecord(recordId);
        LOGGER.info("process flash create recordId:{}", recordId);
    }
}