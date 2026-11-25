package com.macro.mall.portal.service;

import com.macro.mall.common.api.CommonResult;
import com.macro.mall.portal.domain.FlashOrderParam;
import com.macro.mall.portal.domain.FlashOrderResult;
import com.macro.mall.portal.domain.FlashPromotionReconcileItem;

import java.util.List;

/**
 * 秒杀下单。和普通购物车下单是两条路：普通下单依赖 cartIds，秒杀只要「关系ID + 数量」，
 * 价格只能取服务端的 flash_promotion_price，不能信任前端传过来的金额。
 */
public interface FlashPromotionSeckillService {

    /** 预热指定秒杀活动+场次：把剩余库存、已购会员集合、秒杀关系缓存写入Redis */
    int preheat(Long flashPromotionId, Long sessionId);

    /** 预热「当前时间落在场次时间段内且活动/场次均为启用状态」的全部秒杀关系（定时任务使用） */
    int preheatActiveSessions();

    /**
     * 秒杀下单。异步模式（默认）落排队记录后由 MQ 建单，接口返回排队中；
     * 同步模式（mall.flash.async-create=false）在事务内直接建单，返回待支付。
     */
    CommonResult<FlashOrderResult> generateFlashOrder(FlashOrderParam param);

    /** 查询当前会员在某次秒杀中的结果，没抢到（没有成交记录）返回null */
    FlashOrderResult getFlashOrderResult(Long relationId);

    /** 异步建单：MQ 消费者按成交记录ID建单（幂等 + 失败补偿） */
    void createFlashOrderFromRecord(Long recordId);

    /** 兜底扫描：把排队中(0)且创建时间超时的记录重投建单消息，返回重投条数 */
    int resendQueuedFlashOrders();

    /** 兜底扫描：把排队中(0)且超过 5 分钟的记录置为 3（建单失败）并归还 Redis 名额，返回处理条数 */
    int failTimeoutQueuedFlashOrders();

    /** 建单死信队列处理：把仍未建单成功的记录置 3 并归还 Redis 名额，返回是否确实处理 */
    boolean handleFlashOrderCreateDead(Long recordId);

    /** 秒杀对账：比对「Redis 剩余库存/买家数」与「数据库口径」，只反映差异不做修复（可给测试直接调用） */
    List<FlashPromotionReconcileItem> reconcile();

    /** 秒杀订单超时取消：关闭成功才归还名额，重复投递靠状态判断挡掉 */
    boolean handleFlashOrderCancel(Long orderId);
}