package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 秒杀场次自动预热定时器
 * <p>
 * 秒杀下单要求 Redis 中先有库存（否则返回「秒杀未开始」），除了手工调用预热接口外，
 * 这里每分钟扫描一次「当前时间落在场次时间段内且活动/场次均为启用状态」的场次做自动预热。
 * Created by mall-1 seckill v1.
 */
@Component
public class FlashPromotionPreheatTask {
    private final Logger LOGGER = LoggerFactory.getLogger(FlashPromotionPreheatTask.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    /**
     * cron表达式：Seconds Minutes Hours DayofMonth Month DayofWeek [Year]
     * 每分钟第0秒执行一次，预热当前生效场次的 Redis 库存/已购会员/关系缓存
     */
    @Scheduled(cron = "0 * * * * ?")
    private void preheatFlashPromotion() {
        int count = flashPromotionSeckillService.preheatActiveSessions();
        if (count > 0) {
            LOGGER.info("秒杀预热定时任务完成，预热关系数：{}", count);
        }
    }
}