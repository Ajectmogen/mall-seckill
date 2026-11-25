package com.macro.mall.portal.dao;

import com.macro.mall.portal.domain.FlashPromotionRecord;
import com.macro.mall.portal.domain.FlashPromotionRelationCache;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

/** 秒杀相关表的自定义 Dao，SQL 见 classpath:dao/FlashPromotionRecordDao.xml */
public interface FlashPromotionRecordDao {

    /** 插入秒杀成交记录（依赖 uk_relation_member 唯一键做「同一会员同一场次只能抢一次」的最终兜底， 命中唯一键时抛出 DuplicateKeyException） */
    int insert(FlashPromotionRecord record);

    /** 按「秒杀关系+会员」查询成交记录（同一次秒杀只会有一条） */
    FlashPromotionRecord selectByRelationIdAndMemberId(@Param("relationId") Long relationId, @Param("memberId") Long memberId);

    /** 按记录ID查询成交记录（异步建单消费者使用） */
    FlashPromotionRecord selectById(@Param("id") Long id);

    /** 按订单ID查询成交记录（超时取消消费者使用） */
    FlashPromotionRecord selectByOrderId(@Param("orderId") Long orderId);

    /** 统计某个秒杀关系下「占用名额」的成交记录数量之和（quantity 之和），用于预热时计算剩余库存 */
    Integer sumQuantityByRelationId(@Param("relationId") Long relationId);

    /** 统计某个秒杀关系下「占用名额」的成交记录条数，用于对账比对 Redis 买家集合大小 */
    Integer countByRelationId(@Param("relationId") Long relationId);

    /** 查询某个秒杀关系下「占用名额」的成交记录的会员ID列表，预热时用于重建 Redis 已购会员集合 */
    List<Long> selectMemberIdsByRelationId(@Param("relationId") Long relationId);

    /** 异步建单消费前 CAS 领取记录（0排队中 -> 10建单中），影响行数 0 表示已被处理/重复消息 */
    int updateStatusById(@Param("id") Long id, @Param("fromStatus") Integer fromStatus, @Param("toStatus") Integer toStatus);

    /** 回写成交记录上的订单信息，同时把状态推进到 1（待支付） */
    int updateOrderInfoWithStatus(@Param("id") Long id, @Param("orderId") Long orderId,
                                  @Param("orderSn") String orderSn, @Param("status") Integer status);

    /** 建单失败：记录原因、重试计数 +1；带 fromStatus 时做 CAS */
    int updateStatusAndError(@Param("id") Long id, @Param("status") Integer status,
                             @Param("errorMsg") String errorMsg, @Param("fromStatus") Integer fromStatus);

    /** 兜底扫描：查询某状态且创建时间早于 before 的记录ID（按 id 升序，最多 limit 条） */
    List<Long> selectIdsByStatusAndCreateTimeBefore(@Param("status") Integer status,
                                                    @Param("before") Date before,
                                                    @Param("limit") Integer limit);

    /** 按订单ID更新成交记录状态（2=已取消），带状态判断，幂等 */
    int updateStatusByOrderId(@Param("orderId") Long orderId, @Param("status") Integer status);

    /** 建单失败后允许重抢：记录 CAS 回排队中，create_time 一并刷新 */
    int resetToQueued(@Param("id") Long id);

    /** 按关系ID查询秒杀关系（含 product_sku_id、秒杀价、秒杀数量、活动标题） */
    FlashPromotionRelationCache selectRelationById(@Param("relationId") Long relationId);

    /** 查询某个秒杀活动+场次下的全部秒杀关系（预热用） */
    List<FlashPromotionRelationCache> selectRelations(@Param("flashPromotionId") Long flashPromotionId, @Param("sessionId") Long sessionId);

    /** 查询「当前时间落在场次时间段内且活动/场次均为启用状态」的全部秒杀关系（定时预热与对账使用） */
    List<FlashPromotionRelationCache> selectActiveRelations(@Param("currentDate") Date currentDate, @Param("currentTime") String currentTime);
}