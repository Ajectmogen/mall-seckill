package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单取消消息的接收者
 * <p>
 * 使用独立的 mall1.flash.cancel 队列（与普通订单的 mall1.order.cancel 隔离），秒杀的高并发消息不会阻塞普通订单取消。
 * 处理逻辑：按 orderId 取消订单（复用普通订单的幂等取消），并把 sms_flash_promotion_record.status 置 2（已取消），
 * 再把秒杀名额归还 Redis（库存加回 + 会员移出已购集合）——这是「取消后允许该会员重新抢」的产品选择。
 * 注意：数据库侧 uk_relation_member 唯一键仍然存在，同一会员同一场次只会有一条成交记录，
 *      因此「重新抢」在 Redis 层是放行的，但再次下单时仍会被唯一键拦住（详见交付说明）。
 * Created by mall-1 seckill v1.
 */
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