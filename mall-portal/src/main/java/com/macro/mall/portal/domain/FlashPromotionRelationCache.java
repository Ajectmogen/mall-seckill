package com.macro.mall.portal.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;

/** 秒杀关系缓存，写入 mall1:seckill:relation:{relationId}；价格只取服务端这份数据 */
@Getter
@Setter
public class FlashPromotionRelationCache implements Serializable {
    private static final long serialVersionUID = 1L;

    @Schema(title = "秒杀关系ID")
    private Long relationId;
    @Schema(title = "秒杀活动ID")
    private Long flashPromotionId;
    @Schema(title = "秒杀场次ID")
    private Long flashPromotionSessionId;
    @Schema(title = "商品ID")
    private Long productId;
    @Schema(title = "商品SKU编号")
    private Long productSkuId;
    @Schema(title = "秒杀价格")
    private BigDecimal flashPromotionPrice;
    @Schema(title = "秒杀总数量")
    private Integer flashPromotionCount;
    @Schema(title = "秒杀限购数量")
    private Integer flashPromotionLimit;
    @Schema(title = "秒杀活动标题（仅用于订单商品行的促销名称展示）")
    private String flashPromotionTitle;
    @Schema(title = "场次开始时间 HH:mm:ss（用于下单时校验是否在场次时间段内）")
    private String sessionStartTime;
    @Schema(title = "场次结束时间 HH:mm:ss（同时用于计算Redis key的TTL）")
    private String sessionEndTime;
}