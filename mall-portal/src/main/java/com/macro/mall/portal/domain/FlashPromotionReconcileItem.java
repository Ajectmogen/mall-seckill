package com.macro.mall.portal.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 秒杀对账明细
 * <p>
 * 用于 {@code FlashPromotionReconcileTask} 比对「Redis 剩余库存/买家数」与「数据库口径」，
 * 不一致时只打 WARN 日志、不做自动修复（避免误伤正常在途请求）。
 * Created by mall-1 seckill v1.
 */
@Getter
@Setter
public class FlashPromotionReconcileItem implements Serializable {
    private static final long serialVersionUID = 1L;

    @Schema(title = "秒杀关系ID")
    private Long relationId;
    @Schema(title = "Redis剩余库存；null表示key不存在（未预热或已过期）")
    private Integer redisStock;
    @Schema(title = "数据库口径剩余库存 = 秒杀总数量 - 未取消成交记录数量之和")
    private Integer dbStock;
    @Schema(title = "Redis已购会员数")
    private Integer redisBuyers;
    @Schema(title = "数据库未取消成交记录数")
    private Integer dbBuyers;
    @Schema(title = "是否一致")
    private Boolean consistent;

    public static FlashPromotionReconcileItem of(Long relationId, Integer redisStock, Integer dbStock,
                                                 Integer redisBuyers, Integer dbBuyers) {
        FlashPromotionReconcileItem item = new FlashPromotionReconcileItem();
        item.setRelationId(relationId);
        item.setRedisStock(redisStock);
        item.setDbStock(dbStock);
        item.setRedisBuyers(redisBuyers);
        item.setDbBuyers(dbBuyers);
        item.setConsistent(java.util.Objects.equals(redisStock, dbStock) && java.util.Objects.equals(redisBuyers, dbBuyers));
        return item;
    }

    @Override
    public String toString() {
        return "relationId=" + relationId
                + ", redisStock=" + redisStock + ", dbStock=" + dbStock
                + ", redisBuyers=" + redisBuyers + ", dbBuyers=" + dbBuyers;
    }
}