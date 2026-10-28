package com.macro.mall.portal.service;

import com.macro.mall.common.api.CommonResult;
import com.macro.mall.portal.domain.FlashOrderParam;
import com.macro.mall.portal.domain.FlashOrderResult;
import com.macro.mall.portal.domain.FlashPromotionReconcileItem;

import java.util.List;

/**
 * 秒杀（限时购）下单Service
 * <p>
 * 秒杀下单与普通购物车下单完全分离（不复用 generateOrder(OrderParam)）：普通下单依赖 cartIds，
 * 秒杀只需要「秒杀关系ID + 数量」，并且价格只能取服务端的 flash_promotion_price。
 * Created by mall-1 seckill v1.
 */
public interface FlashPromotionSeckillService {

    /**
     * 预热指定秒杀活动+场次：把剩余库存、已购会员集合、秒杀关系缓存写入Redis
     *
     * @return 预热处理的秒杀关系条数
     */
    int preheat(Long flashPromotionId, Long sessionId);

    /**
     * 预热「当前时间落在场次时间段内且活动/场次均为启用状态」的全部秒杀关系（定时任务使用）
     *
     * @return 预热处理的秒杀关系条数
     */
    int preheatActiveSessions();

    /**
     * 秒杀下单（v2）
     * <p>
     * 异步模式（mall.flash.async-create=true，默认）：Lua 预扣 + 短事务落一条 status=0（排队中）的成交记录 +
     * 事务提交后投递建单消息，接口立即返回 status=0/排队中，订单由 MQ 消费者异步创建；
     * 同步模式（false）：保留 v1 行为，在同一事务内锁库存+建单，返回 status=1（待支付）。
     * <p>
     * 业务性失败（未开始/库存不足/重复下单）返回 {@code CommonResult.failed}，
     * 参数性错误使用 {@code Asserts.fail}
     */
    CommonResult<FlashOrderResult> generateFlashOrder(FlashOrderParam param);

    /**
     * 查询当前会员在某次秒杀中的结果，没抢到（没有成交记录）返回null。
     * v2 状态机：0排队中/10建单中/1待支付/2已取消/3建单失败/4已支付
     */
    FlashOrderResult getFlashOrderResult(Long relationId);

    /**
     * 异步建单：MQ 消费者按成交记录ID建单（幂等 + 失败补偿）
     */
    void createFlashOrderFromRecord(Long recordId);

    /**
     * 兜底扫描：把排队中(0)且创建时间超时的记录重投建单消息，返回重投条数
     */
    int resendQueuedFlashOrders();

    /**
     * 兜底扫描：把排队中(0)且超过 5 分钟的记录置为 3（建单失败）并归还 Redis 名额，返回处理条数
     */
    int failTimeoutQueuedFlashOrders();

    /**
     * 建单死信队列处理：把仍未建单成功的记录置 3 并归还 Redis 名额，返回是否确实处理
     */
    boolean handleFlashOrderCreateDead(Long recordId);

    /**
     * 秒杀对账：比对「Redis 剩余库存/买家数」与「数据库口径」，只反映差异不做修复（可给测试直接调用）
     */
    List<FlashPromotionReconcileItem> reconcile();

    /**
     * 处理秒杀订单超时取消（秒杀专用延迟队列消费者调用）
     * <p>
     * 订单被成功关闭时归还 Redis 秒杀库存并把会员移出已购集合，保证「取消后可重新参与」（Redis 层面）；
     * 重复投递、订单已支付等场景幂等返回。
     *
     * @return true 表示本次调用确实执行了取消/归还
     */
    boolean handleFlashOrderCancel(Long orderId);
}