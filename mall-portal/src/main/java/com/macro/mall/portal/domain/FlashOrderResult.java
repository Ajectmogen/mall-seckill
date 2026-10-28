package com.macro.mall.portal.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 秒杀下单结果 / 秒杀结果查询返回对象
 * Created by mall-1 seckill v1.
 */
@Getter
@Setter
public class FlashOrderResult {
    @Schema(title = "秒杀请求ID（= sms_flash_promotion_record.id，异步模式下用于轮询/排查）")
    private Long requestId;
    @Schema(title = "秒杀关系ID")
    private Long relationId;
    @Schema(title = "订单ID")
    private Long orderId;
    @Schema(title = "订单编号")
    private String orderSn;
    @Schema(title = "会员ID")
    private Long memberId;
    @Schema(title = "商品ID")
    private Long productId;
    @Schema(title = "商品SKU编号")
    private Long productSkuId;
    @Schema(title = "秒杀价格（服务端从关系表读取）")
    private BigDecimal flashPromotionPrice;
    @Schema(title = "购买数量")
    private Integer quantity;
    @Schema(title = "秒杀成交记录状态：0排队中；10建单中；1待支付；2已取消；3建单失败；4已支付")
    private Integer status;
    @Schema(title = "订单状态：0待付款；1待发货；2已发货；3已完成；4已关闭；5无效")
    private Integer orderStatus;
    @Schema(title = "状态中文描述：排队中/建单中/待支付/已取消/建单失败/已支付")
    private String statusDesc;
    @Schema(title = "建单失败原因（status=3 时有值，其余为 null）")
    private String errorMsg;
    @Schema(title = "秒杀记录创建时间")
    private Date createTime;
}