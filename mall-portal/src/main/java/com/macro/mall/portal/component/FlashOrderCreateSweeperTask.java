package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 兜底扫描：重投超时未消费的排队记录，超时过久则判失败并归还名额 */
@Component
public class FlashOrderCreateSweeperTask {
    private final Logger LOGGER = LoggerFactory.getLogger(FlashOrderCreateSweeperTask.class);
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;
    @Value("${mall.flash.async-create:true}")
    private boolean asyncCreate;

    @Scheduled(fixedDelay = 10000)
    public void sweepQueuedFlashOrders() {
        if (!asyncCreate) {
            return;
        }
        try {
            int resent = flashPromotionSeckillService.resendQueuedFlashOrders();
            if (resent > 0) {
                LOGGER.warn("秒杀建单兜底扫描：重投 {} 条排队超过15秒的记录", resent);
            }
        } catch (Exception e) {
            LOGGER.error("秒杀建单兜底扫描-重投失败", e);
        }
        try {
            int failed = flashPromotionSeckillService.failTimeoutQueuedFlashOrders();
            if (failed > 0) {
                LOGGER.error("秒杀建单兜底扫描：{} 条排队超过5分钟的记录已置为建单失败并归还Redis名额", failed);
            }
        } catch (Exception e) {
            LOGGER.error("秒杀建单兜底扫描-超时判失败失败", e);
        }
    }
}