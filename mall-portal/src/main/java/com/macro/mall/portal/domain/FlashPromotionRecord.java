package com.macro.mall.portal.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/** 秒杀成交记录，对应 sms_flash_promotion_record 表 */
@Getter
@Setter
public class FlashPromotionRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @Schema(title = "记录ID")
    private Long id;
    @Schema(title = "秒杀关系ID")
    private Long relationId;
    @Schema(title = "会员ID")
    private Long memberId;
    @Schema(title = "商品ID")
    private Long productId;
    @Schema(title = "商品SKU编号")
    private Long productSkuId;
    @Schema(title = "订单ID")
    private Long orderId;
    @Schema(title = "订单编号")
    private String orderSn;
    @Schema(title = "购买数量")
    private Integer quantity;
    @Schema(title = "状态：0排队中；10建单中；1待支付；2已取消；3建单失败；4已支付")
    private Integer status;
    @Schema(title = "建单失败原因（status=3 时写入）")
    private String errorMsg;
    @Schema(title = "建单重试次数（失败补偿时+1）")
    private Integer retryCount;
    @Schema(title = "创建时间")
    private Date createTime;
}