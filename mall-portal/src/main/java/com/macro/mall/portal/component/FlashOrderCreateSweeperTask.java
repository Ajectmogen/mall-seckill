package com.macro.mall.portal.component;

import com.macro.mall.portal.service.FlashPromotionSeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 秒杀建单兜底扫描任务（v2）
 * <p>
 * 解决「消息丢了」的问题（MQ 宕机、发送失败、消息被误删）：
 * <ol>
 *   <li>每 10 秒扫描 status=0（排队中）且 create_time 早于 15 秒前的记录，重投建单消息；
 *       重投是幂等的（消费者 CAS 0->10 只有一次能成功），所以重投无害；</li>
 *   <li>把超过 5 分钟仍是 status=0 的记录置 3（建单失败）并归还 Redis 名额，避免永久卡在"排队中"。</li>
 * </ol>
 * 同步模式（mall.flash.async-create=false）没有排队记录，任务直接跳过。
 * Created by mall-1 seckill v2.
 */
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