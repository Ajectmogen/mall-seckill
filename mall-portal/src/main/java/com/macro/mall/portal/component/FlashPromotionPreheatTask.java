package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 每分钟预热当前生效场次，把库存、已购集合、关系缓存写进 Redis */
@Component
public class FlashPromotionPreheatTask {
    private final Logger LOGGER = LoggerFactory.getLogger(FlashPromotionPreheatTask.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    /** cron表达式：Seconds Minutes Hours DayofMonth Month DayofWeek [Year] 每分钟第0秒执行一次，预热当前生效场次的 Redis */
    @Scheduled(cron = "0 * * * * ?")
    private void preheatFlashPromotion() {
        int count = flashPromotionSeckillService.preheatActiveSessions();
        if (count > 0) {
            LOGGER.info("秒杀预热定时任务完成，预热关系数：{}", count);
        }
    }
}