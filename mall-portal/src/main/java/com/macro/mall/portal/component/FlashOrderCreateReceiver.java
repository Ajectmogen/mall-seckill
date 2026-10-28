package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 秒杀异步建单消息的接收者（v2）
 * <p>
 * 消费流程全部在 {@link FlashPromotionSeckillService#createFlashOrderFromRecord(Long)} 内：
 * 先 CAS 领取（0->10）保证幂等，再在事务内锁库存 + 建单 + 回写记录；
 * 失败时补偿 Redis 并把记录置 3（建单失败），**不向外抛异常**，避免消息被无限重投。
 * Created by mall-1 seckill v2.
 */
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