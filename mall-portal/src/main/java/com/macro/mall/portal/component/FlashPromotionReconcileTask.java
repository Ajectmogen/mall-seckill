package com.macro.mall.portal.component;

import cn.hutool.core.collection.CollUtil;
import com.macro.mall.portal.domain.FlashPromotionReconcileItem;
import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 秒杀库存对账定时器
 * <p>
 * 每5分钟比对「Redis 剩余库存 + Redis 买家数」与「数据库口径：flash_promotion_count - 未取消成交记录数量」，
 * 不一致只打 WARN 明细，**不做自动修复**：Redis 扣减与成交记录落库之间存在毫秒级时间窗口，
 * 瞬时不一致属于正常现象，自动修复容易误伤正在下单的请求（下一分钟的预热会按数据库口径自然纠偏）。
 * Created by mall-1 seckill v1.
 */
@Component
public class FlashPromotionReconcileTask {
    private final Logger LOGGER = LoggerFactory.getLogger(FlashPromotionReconcileTask.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    /**
     * cron表达式：Seconds Minutes Hours DayofMonth Month DayofWeek [Year]
     * 每5分钟执行一次
     */
    @Scheduled(cron = "0 0/5 * * * ?")
    private void reconcileFlashPromotion() {
        List<FlashPromotionReconcileItem> items = flashPromotionSeckillService.reconcile();
        if (CollUtil.isEmpty(items)) {
            LOGGER.info("秒杀对账：当前没有生效中的秒杀场次");
            return;
        }
        int inconsistent = 0;
        for (FlashPromotionReconcileItem item : items) {
            if (Boolean.TRUE.equals(item.getConsistent())) {
                continue;
            }
            inconsistent++;
            if (item.getRedisStock() == null) {
                //库存key不存在说明该场次还没预热（或key已过期），不算数据不一致
                LOGGER.info("秒杀对账：relationId:{} 尚未预热，数据库口径剩余库存:{}", item.getRelationId(), item.getDbStock());
            } else {
                LOGGER.warn("秒杀对账不一致：{}", item);
            }
        }
        LOGGER.info("秒杀对账完成，生效关系数：{}，不一致数：{}", items.size(), inconsistent);
    }
}