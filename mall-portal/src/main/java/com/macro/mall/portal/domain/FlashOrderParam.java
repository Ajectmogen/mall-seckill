package com.macro.mall.portal.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

/**
 * 秒杀下单请求参数
 * Created by mall-1 seckill v1.
 */
@Getter
@Setter
public class FlashOrderParam {
    @Schema(title = "秒杀关系ID（sms_flash_promotion_product_relation.id）", requiredMode = Schema.RequiredMode.REQUIRED)
    private Long relationId;
    @Schema(title = "购买数量，不传或小于1时按1处理", defaultValue = "1")
    private Integer quantity;
}