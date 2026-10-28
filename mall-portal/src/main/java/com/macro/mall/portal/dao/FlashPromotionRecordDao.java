package com.macro.mall.portal.dao;

import com.macro.mall.portal.domain.FlashPromotionRecord;
import com.macro.mall.portal.domain.FlashPromotionRelationCache;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

/**
 * 秒杀（限时购）自定义Dao
 * <p>
 * 说明：秒杀新增的 sms_flash_promotion_record 表与 sms_flash_promotion_product_relation.product_sku_id 字段
 * 都没有进 mall-mbg，因此统一放在 mall-portal 自己的 dao 包内，
 * XML 定义在 classpath:dao/FlashPromotionRecordDao.xml，由 MyBatisConfig 的 mapperLocations 自动扫描加载。
 * Created by mall-1 seckill v1.
 */
public interface FlashPromotionRecordDao {

    /**
     * 插入秒杀成交记录（依赖 uk_relation_member 唯一键做「同一会员同一场次只能抢一次」的最终兜底，
     * 命中唯一键时抛出 DuplicateKeyException）
     */
    int insert(FlashPromotionRecord record);

    /**
     * 按「秒杀关系+会员」查询成交记录（同一次秒杀只会有一条）
     */
    FlashPromotionRecord selectByRelationIdAndMemberId(@Param("relationId") Long relationId, @Param("memberId") Long memberId);

    /**
     * 按记录ID查询成交记录（异步建单消费者使用）
     */
    FlashPromotionRecord selectById(@Param("id") Long id);

    /**
     * 按订单ID查询成交记录（超时取消消费者使用）
     */
    FlashPromotionRecord selectByOrderId(@Param("orderId") Long orderId);

    /**
     * 统计某个秒杀关系下「占用名额」的成交记录数量之和（quantity 之和），用于预热时计算剩余库存。
     * 占用名额口径：status IN (0排队中,10建单中,1待支付,4已支付)；2已取消与3建单失败都不占名额。
     */
    Integer sumQuantityByRelationId(@Param("relationId") Long relationId);

    /**
     * 统计某个秒杀关系下「占用名额」的成交记录条数，用于对账比对 Redis 买家集合大小
     */
    Integer countByRelationId(@Param("relationId") Long relationId);

    /**
     * 查询某个秒杀关系下「占用名额」的成交记录的会员ID列表，预热时用于重建 Redis 已购会员集合
     */
    List<Long> selectMemberIdsByRelationId(@Param("relationId") Long relationId);

    /**
     * 异步建单消费前 CAS 领取记录（0排队中 -> 10建单中），影响行数 0 表示已被处理/重复消息
     */
    int updateStatusById(@Param("id") Long id, @Param("fromStatus") Integer fromStatus, @Param("toStatus") Integer toStatus);

    /**
     * 回写成交记录上的订单信息，同时把状态推进到 1（待支付）
     */
    int updateOrderInfoWithStatus(@Param("id") Long id, @Param("orderId") Long orderId,
                                  @Param("orderSn") String orderSn, @Param("status") Integer status);

    /**
     * 建单失败：置状态（3建单失败）并记录失败原因，retry_count+1；fromStatus 非空时要求状态未变（CAS，保证幂等）
     */
    int updateStatusAndError(@Param("id") Long id, @Param("status") Integer status,
                             @Param("errorMsg") String errorMsg, @Param("fromStatus") Integer fromStatus);

    /**
     * 兜底扫描：查询某状态且创建时间早于 before 的记录ID（按 id 升序，最多 limit 条）
     */
    List<Long> selectIdsByStatusAndCreateTimeBefore(@Param("status") Integer status,
                                                    @Param("before") Date before,
                                                    @Param("limit") Integer limit);

    /**
     * 按订单ID更新成交记录状态（2=已取消），带状态判断，幂等
     */
    int updateStatusByOrderId(@Param("orderId") Long orderId, @Param("status") Integer status);

    /**
     * 建单失败(3)后允许会员重新抢：CAS 把记录改回排队中(0)，清空失败原因、retry_count+1，
     * 并把 create_time 刷成当前时间（避免刚重试就被兜底扫描按「创建超过5分钟」判失败）。
     * 影响行数 0 = 状态已不是 3（并发重试/已被处理）
     */
    int resetToQueued(@Param("id") Long id);

    /**
     * 按关系ID查询秒杀关系（含 product_sku_id、秒杀价、秒杀数量、活动标题）
     */
    FlashPromotionRelationCache selectRelationById(@Param("relationId") Long relationId);

    /**
     * 查询某个秒杀活动+场次下的全部秒杀关系（预热用）
     */
    List<FlashPromotionRelationCache> selectRelations(@Param("flashPromotionId") Long flashPromotionId, @Param("sessionId") Long sessionId);

    /**
     * 查询「当前时间落在场次时间段内且活动/场次均为启用状态」的全部秒杀关系（定时预热与对账使用）
     *
     * @param currentDate 当前日期，用于比较 sms_flash_promotion.start_date / end_date（DATE 列）
     * @param currentTime 当前时间字符串 HH:mm:ss，用于比较 session 的 TIME 列（避免 java.util.Date 与 TIME 列比较的时区/日期陷阱）
     */
    List<FlashPromotionRelationCache> selectActiveRelations(@Param("currentDate") Date currentDate, @Param("currentTime") String currentTime);
}