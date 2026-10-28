package com.macro.mall.portal.controller;

import com.macro.mall.common.api.CommonPage;
import com.macro.mall.common.api.CommonResult;
import com.macro.mall.portal.domain.ConfirmOrderResult;
import com.macro.mall.portal.domain.FlashOrderParam;
import com.macro.mall.portal.domain.FlashOrderResult;
import com.macro.mall.portal.domain.FlashPromotionReconcileItem;
import com.macro.mall.portal.domain.OmsOrderDetail;
import com.macro.mall.portal.domain.OrderParam;
import com.macro.mall.portal.service.FlashPromotionSeckillService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.UmsMemberService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 订单管理Controller
 * Created by macro on 2018/8/30.
 */
@Controller
@Tag(name = "OmsPortalOrderController", description = "订单管理")
@RequestMapping("/order")
public class OmsPortalOrderController {
    @Autowired
    private OmsPortalOrderService portalOrderService;
    @Autowired
    private UmsMemberService memberService;
    @Autowired
    private FlashPromotionSeckillService flashPromotionSeckillService;

    @Operation(summary = "根据购物车信息生成确认单")
    @RequestMapping(value = "/generateConfirmOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult<ConfirmOrderResult> generateConfirmOrder(@RequestBody List<Long> cartIds) {
        ConfirmOrderResult confirmOrderResult = portalOrderService.generateConfirmOrder(cartIds);
        return CommonResult.success(confirmOrderResult);
    }

    @Operation(summary = "根据购物车信息生成订单")
    @RequestMapping(value = "/generateOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult generateOrder(@RequestBody OrderParam orderParam) {
        Map<String, Object> result = portalOrderService.generateOrder(orderParam);
        return CommonResult.success(result, "下单成功");
    }

    @Operation(summary = "秒杀下单", description = "入参为秒杀关系ID(relationId)与购买数量(quantity)；价格由服务端从秒杀关系表读取，不信任前端")
    @RequestMapping(value = "/generateFlashOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult<FlashOrderResult> generateFlashOrder(@RequestBody FlashOrderParam flashOrderParam) {
        //成功与失败都用 CommonResult 承载，失败文案见 FlashPromotionSeckillServiceImpl
        return flashPromotionSeckillService.generateFlashOrder(flashOrderParam);
    }

    @Operation(summary = "查询当前登录会员的秒杀结果", description = "按秒杀关系ID查询本人的秒杀成交记录；没抢到或记录不存在返回 data 为 null 的成功响应")
    @RequestMapping(value = "/flashOrderResult", method = RequestMethod.GET)
    @ResponseBody
    public CommonResult<FlashOrderResult> flashOrderResult(@RequestParam Long relationId) {
        FlashOrderResult result = flashPromotionSeckillService.getFlashOrderResult(relationId);
        return CommonResult.success(result);
    }

    @Operation(summary = "轮询秒杀建单结果(v2)", description = "与 /flashOrderResult 同一实现：返回 status(0排队中/10建单中/1待支付/2已取消/3建单失败/4已支付)"
            + "、statusDesc、requestId、orderSn/orderId、errorMsg；前端提交后按 500ms 间隔轮询直到 status 不再为 0/10")
    @RequestMapping(value = "/flash/result", method = RequestMethod.GET)
    @ResponseBody
    public CommonResult<FlashOrderResult> flashResult(@RequestParam Long relationId) {
        FlashOrderResult result = flashPromotionSeckillService.getFlashOrderResult(relationId);
        return CommonResult.success(result);
    }

    @Operation(summary = "预热秒杀场次(演示/测试用)",
            description = "把指定活动+场次下所有秒杀关系的库存与价格写入 Redis。正常情况由 FlashPromotionPreheatTask 每分钟自动预热，此接口仅供演示与压测前手工预热")
    @RequestMapping(value = "/flash/preheat", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult<Integer> preheatFlashPromotion(@RequestParam Long flashPromotionId, @RequestParam Long sessionId) {
        Integer count = flashPromotionSeckillService.preheat(flashPromotionId, sessionId);
        return CommonResult.success(count, "预热成功");
    }

    @Operation(summary = "秒杀库存对账(演示/测试用)",
            description = "比对 Redis 剩余库存/买家数与数据库口径，返回每条秒杀关系的明细；只读不修复，正式环境由 FlashPromotionReconcileTask 每5分钟打日志")
    @RequestMapping(value = "/flash/reconcile", method = RequestMethod.GET)
    @ResponseBody
    public CommonResult<List<FlashPromotionReconcileItem>> reconcileFlashPromotion() {
        return CommonResult.success(flashPromotionSeckillService.reconcile());
    }

    @Operation(summary = "用户支付成功的回调")
    @RequestMapping(value = "/paySuccess", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult paySuccess(@RequestParam Long orderId,@RequestParam Integer payType) {
        OmsOrderDetail order = portalOrderService.detail(orderId);
        //🩹 订单不存在时直接返回业务错误，避免 order 为 null 时调用 getMemberId() 触发 NPE（HTTP 500）
        if (order == null) {
            return CommonResult.failed("订单不存在");
        }
        //🩹 常量/当前用户在前：即使订单的 member_id 为 null 也只会得到 403，不会 NPE
        if (!memberService.getCurrentMember().getId().equals(order.getMemberId())) {
            return CommonResult.forbidden("无权操作他人订单");
        }
        Integer count = portalOrderService.paySuccess(orderId,payType);
        return CommonResult.success(count, "支付成功");
    }

    @Operation(summary = "自动取消超时订单")
    @RequestMapping(value = "/cancelTimeOutOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult cancelTimeOutOrder() {
        portalOrderService.cancelTimeOutOrder();
        return CommonResult.success(null);
    }

    @Operation(summary = "取消单个超时订单")
    @RequestMapping(value = "/cancelOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult cancelOrder(Long orderId) {
        portalOrderService.sendDelayMessageCancelOrder(orderId);
        return CommonResult.success(null);
    }

    @Operation(summary = "按状态分页获取用户订单列表")
    @Parameter(name = "status", description = "订单状态：-1->全部；0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭",
            in = ParameterIn.QUERY, schema = @Schema(type = "integer",defaultValue = "-1",allowableValues = {"-1","0","1","2","3","4"}))
    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @ResponseBody
    public CommonResult<CommonPage<OmsOrderDetail>> list(@RequestParam Integer status,
                                                   @RequestParam(required = false, defaultValue = "1") Integer pageNum,
                                                   @RequestParam(required = false, defaultValue = "5") Integer pageSize) {
        CommonPage<OmsOrderDetail> orderPage = portalOrderService.list(status,pageNum,pageSize);
        return CommonResult.success(orderPage);
    }

    @Operation(summary = "根据ID获取订单详情")
    @RequestMapping(value = "/detail/{orderId}", method = RequestMethod.GET)
    @ResponseBody
    public CommonResult<OmsOrderDetail> detail(@PathVariable Long orderId) {
        OmsOrderDetail orderDetail = portalOrderService.detail(orderId);
        //🩹 同上：先判空，再用"当前会员 id"作为 equals 的调用方，避免订单不存在或 member_id 为空时 NPE(500)
        if (orderDetail == null) {
            return CommonResult.failed("订单不存在");
        }
        if (!memberService.getCurrentMember().getId().equals(orderDetail.getMemberId())) {
            return CommonResult.forbidden("无权查看他人订单");
        }
        return CommonResult.success(orderDetail);
    }

    @Operation(summary = "用户取消订单")
    @RequestMapping(value = "/cancelUserOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult cancelUserOrder(Long orderId) {
        OmsOrderDetail order = portalOrderService.detail(orderId);
        //🩹 先判空再比较归属，避免订单不存在/member_id 为空时 NPE(500)
        if (order == null) {
            return CommonResult.failed("订单不存在");
        }
        if (!memberService.getCurrentMember().getId().equals(order.getMemberId())) {
            return CommonResult.forbidden("无权操作他人订单");
        }
        portalOrderService.cancelOrder(orderId);
        return CommonResult.success(null);
    }

    @Operation(summary = "用户确认收货")
    @RequestMapping(value = "/confirmReceiveOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult confirmReceiveOrder(Long orderId) {
        portalOrderService.confirmReceiveOrder(orderId);
        return CommonResult.success(null);
    }

    @Operation(summary = "用户删除订单")
    @RequestMapping(value = "/deleteOrder", method = RequestMethod.POST)
    @ResponseBody
    public CommonResult deleteOrder(Long orderId) {
        portalOrderService.deleteOrder(orderId);
        return CommonResult.success(null);
    }
}
