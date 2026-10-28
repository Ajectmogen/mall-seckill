package com.macro.mall.portal.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.common.api.CommonResult;
import com.macro.mall.common.exception.Asserts;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.mapper.OmsOrderSettingMapper;
import com.macro.mall.mapper.PmsProductMapper;
import com.macro.mall.mapper.PmsSkuStockMapper;
import com.macro.mall.mapper.UmsMemberReceiveAddressMapper;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.model.OmsOrderSetting;
import com.macro.mall.model.OmsOrderSettingExample;
import com.macro.mall.model.PmsProduct;
import com.macro.mall.model.PmsSkuStock;
import com.macro.mall.model.PmsSkuStockExample;
import com.macro.mall.model.UmsMember;
import com.macro.mall.model.UmsMemberReceiveAddress;
import com.macro.mall.model.UmsMemberReceiveAddressExample;
import com.macro.mall.portal.component.FlashOrderCancelSender;
import com.macro.mall.portal.component.FlashOrderCreateSender;
import com.macro.mall.portal.dao.FlashPromotionRecordDao;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.dao.PortalOrderItemDao;
import com.macro.mall.portal.domain.FlashOrderParam;
import com.macro.mall.portal.domain.FlashOrderResult;
import com.macro.mall.portal.domain.FlashPromotionRecord;
import com.macro.mall.portal.domain.FlashPromotionReconcileItem;
import com.macro.mall.portal.domain.FlashPromotionRelationCache;
import com.macro.mall.portal.service.FlashPromotionSeckillService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.UmsMemberService;
import com.macro.mall.portal.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀（限时购）下单Service实现
 * <p>
 * 三层防超卖：
 * <ol>
 *   <li>Redis + Lua 原子预扣（前置拦截，挡住绝大多数并发流量）；</li>
 *   <li>sms_flash_promotion_record 的 uk_relation_member 唯一键（一人一场次一单，数据库最终防线）；</li>
 *   <li>pms_sku_stock 的原子扣减 lockSkuStock（真实库存兜底，扣不动就回滚）。</li>
 * </ol>
 * 说明：落库环节使用编程式事务 TransactionTemplate 而非 @Transactional —— 锁库存需要用影响行数判断失败，
 * 方法内自行 catch 异常时 Spring 声明式事务不会回滚，会导致「订单写了一半」的脏数据，因此显式控制事务边界。
 * Created by mall-1 seckill v1.
 */
@Service
public class FlashPromotionSeckillServiceImpl implements FlashPromotionSeckillService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlashPromotionSeckillServiceImpl.class);

    /**
     * 秒杀Redis key前缀：按约定使用固定字面量（不跟随 redis.database=mall 的配置），
     * 便于压测脚本/运维按 mall1:seckill:* 直接核对。
     */
    private static final String SECKILL_KEY_PREFIX = "mall1:seckill:";
    /**
     * 剩余库存（String）
     */
    private static final String STOCK_KEY_PREFIX = SECKILL_KEY_PREFIX + "stock:";
    /**
     * 已购会员集合（Set）
     */
    private static final String BUYERS_KEY_PREFIX = SECKILL_KEY_PREFIX + "buyers:";
    /**
     * 秒杀关系缓存（String，JSON）
     */
    private static final String RELATION_KEY_PREFIX = SECKILL_KEY_PREFIX + "relation:";

    /**
     * 订单号中的订单来源/支付方式（沿用普通订单的编码：1->app订单、1->支付宝）
     */
    private static final int FLASH_ORDER_SOURCE_TYPE = 1;
    private static final int FLASH_ORDER_PAY_TYPE = 1;

    /**
     * 库存/买家key的最小TTL（秒）：场次已结束时不让key长期驻留，否则场次结束后仍可能下单
     */
    private static final long MIN_KEY_TTL_SECONDS = 300L;

    /**
     * 读不到 oms_order_setting.flash_order_overtime 时的兜底超时分钟数
     */
    private static final int DEFAULT_FLASH_ORDER_OVERTIME_MINUTES = 60;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * 秒杀成交记录状态机（v2）：0排队中（已扣Redis名额、消息未消费）→ 10建单中（消费者CAS领取）
     * → 1待支付（建单成功）→ 2已取消 / 4已支付；3建单失败（名额已还回Redis，可重新抢）。
     * 占用名额口径：0/10/1/4；2与3都不占名额。
     */
    private static final int STATUS_QUEUED = 0;
    private static final int STATUS_CREATING = 10;
    private static final int STATUS_UNPAID = 1;
    private static final int STATUS_CANCELED = 2;
    private static final int STATUS_CREATE_FAILED = 3;
    private static final int STATUS_PAID = 4;

    /**
     * 兜底扫描：排队中的记录超过该秒数仍未建单则重投MQ消息
     */
    private static final int SWEEP_RESEND_SECONDS = 15;

    /**
     * 兜底扫描：排队中的记录超过该分钟数仍未建单则直接判失败并归还Redis名额（避免永久卡在排队中）
     */
    private static final int SWEEP_TIMEOUT_MINUTES = 5;

    /**
     * 兜底扫描每次最多处理的记录数
     */
    private static final int SWEEP_BATCH_SIZE = 200;

    @Autowired
    private FlashPromotionRecordDao flashPromotionRecordDao;
    @Autowired
    private PortalOrderDao portalOrderDao;
    @Autowired
    private PortalOrderItemDao portalOrderItemDao;
    @Autowired
    private OmsOrderMapper omsOrderMapper;
    @Autowired
    private OmsOrderSettingMapper omsOrderSettingMapper;
    @Autowired
    private PmsProductMapper pmsProductMapper;
    @Autowired
    private PmsSkuStockMapper pmsSkuStockMapper;
    @Autowired
    private UmsMemberService umsMemberService;
    @Autowired
    private UmsMemberReceiveAddressMapper memberReceiveAddressMapper;
    @Autowired
    private OmsPortalOrderService portalOrderService;
    @Autowired
    private FlashOrderCancelSender flashOrderCancelSender;
    @Autowired
    private FlashOrderCreateSender flashOrderCreateSender;
    /**
     * 秒杀异步建单开关（v2）：true=Lua预扣+排队落记录+MQ异步建单；false=v1 同步建单（压测对比用）
     */
    @Value("${mall.flash.async-create:true}")
    private boolean asyncCreate;
    /**
     * 必须使用 Spring Boot 自带的 StringRedisTemplate（String 序列化），
     * 不能用 mall-common 的 RedisService（Jackson 值序列化会把 Lua 参数变成带引号的 JSON，tonumber 直接报错）
     */
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private DefaultRedisScript<Long> seckillScript;
    @Autowired
    private ObjectMapper objectMapper;
    @Value("${redis.database}")
    private String redisDatabase;
    @Value("${redis.key.orderId}")
    private String redisKeyOrderId;

    private final TransactionTemplate transactionTemplate;

    @Autowired
    public FlashPromotionSeckillServiceImpl(PlatformTransactionManager transactionManager) {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Override
    public int preheat(Long flashPromotionId, Long sessionId) {
        if (flashPromotionId == null || sessionId == null) {
            Asserts.fail("秒杀活动ID和场次ID不能为空");
        }
        List<FlashPromotionRelationCache> relations = flashPromotionRecordDao.selectRelations(flashPromotionId, sessionId);
        if (CollUtil.isEmpty(relations)) {
            LOGGER.warn("秒杀预热：活动{}场次{}下没有配置秒杀商品", flashPromotionId, sessionId);
            return 0;
        }
        return preheatRelations(relations);
    }

    @Override
    public int preheatActiveSessions() {
        List<FlashPromotionRelationCache> relations = flashPromotionRecordDao.selectActiveRelations(DateUtil.getDate(new Date()), currentTimeString());
        if (CollUtil.isEmpty(relations)) {
            return 0;
        }
        return preheatRelations(relations);
    }

    private int preheatRelations(List<FlashPromotionRelationCache> relations) {
        int count = 0;
        for (FlashPromotionRelationCache relation : relations) {
            try {
                preheatRelation(relation);
                count++;
            } catch (Exception e) {
                //单条关系预热失败不能影响其它秒杀商品（Redis 抖动时定时任务还要继续跑）
                LOGGER.error("秒杀预热失败，relationId:{}", relation.getRelationId(), e);
            }
        }
        return count;
    }

    /**
     * 预热单条秒杀关系：
     * 1) 库存 key 只在**不存在**时按数据库口径建立：剩余库存 = flash_promotion_count - 占用名额的成交记录数量之和
     *    （v2 占用名额口径：status IN (0排队中,10建单中,1待支付,4已支付)；2已取消与3建单失败都不占名额）；
     * 2) 已购会员集合只在「库存 key 是新建的」或「集合本身不存在」时按数据库口径重建（同一占用名额口径）；
     * 3) 缓存秒杀关系 JSON，下单时直接取价格/SKU，不必每单回源数据库。
     * 注意：抢购进行中**绝不做覆盖写**。Redis 扣减与成交记录落库之间有毫秒级窗口，若此处无条件覆盖写，
     * 就会把「已在 Redis 扣减、但还没提交到数据库」的那几件库存还回去，让别的会员多抢 —— 那是真实超发。
     * 所以库存 key 一旦建立就以 Redis 的原子扣减为准；只有 key 丢失时（应用重启、Redis 抖动、场次 key 过期）
     * 才按数据库口径重建，重建本身也是原子的 SETNX，不会覆盖并发抢购中的扣减结果。
     */
    private void preheatRelation(FlashPromotionRelationCache relation) {
        Long relationId = relation.getRelationId();
        long ttl = resolveTtlSeconds(relation);
        int total = relation.getFlashPromotionCount() == null ? 0 : relation.getFlashPromotionCount();
        Integer sold = flashPromotionRecordDao.sumQuantityByRelationId(relationId);
        int remain = Math.max(total - (sold == null ? 0 : sold), 0);
        String stockKey = stockKey(relationId);
        String buyersKey = buyersKey(relationId);
        //库存：SETNX，只在 key 不存在时建立；抢购进行中绝不覆盖，避免把「已扣减未落库」的库存还回去
        Boolean freshStock = stringRedisTemplate.opsForValue()
                .setIfAbsent(stockKey, String.valueOf(remain), ttl, TimeUnit.SECONDS);
        //已购会员：仅在库存 key 新建、或集合不存在时重建，避免抢购过程中清空集合造成同一会员被重复放行
        boolean freshBuyers = !Boolean.TRUE.equals(stringRedisTemplate.hasKey(buyersKey));
        if (Boolean.TRUE.equals(freshStock) || freshBuyers) {
            List<Long> memberIds = flashPromotionRecordDao.selectMemberIdsByRelationId(relationId);
            stringRedisTemplate.delete(buyersKey);
            if (CollUtil.isNotEmpty(memberIds)) {
                String[] members = memberIds.stream().map(String::valueOf).toArray(String[]::new);
                stringRedisTemplate.opsForSet().add(buyersKey, members);
            }
        }
        stringRedisTemplate.expire(buyersKey, ttl, TimeUnit.SECONDS);
        //关系缓存
        String json = toJson(relation);
        if (json != null) {
            stringRedisTemplate.opsForValue().set(relationKey(relationId), json, ttl, TimeUnit.SECONDS);
        }
        LOGGER.info("秒杀预热完成，relationId:{}, 总数量:{}, 已占用:{}, 剩余:{}, 库存key新建:{}, ttl:{}秒",
                relationId, total, sold, remain, Boolean.TRUE.equals(freshStock), ttl);
    }

    @Override
    public CommonResult<FlashOrderResult> generateFlashOrder(FlashOrderParam param) {
        if (param == null || param.getRelationId() == null) {
            Asserts.fail("秒杀关系ID不能为空");
        }
        Long relationId = param.getRelationId();
        //会员登录校验（未登录时 getCurrentMember 会抛「暂未登录或token已经过期」）
        UmsMember member = umsMemberService.getCurrentMember();
        Long memberId = member.getId();
        int requestQuantity = (param.getQuantity() == null || param.getQuantity() < 1) ? 1 : param.getQuantity();
        //1.取秒杀关系（Redis缓存 → 回源数据库并回填），价格只认服务端这份数据
        FlashPromotionRelationCache relation = getRelationCache(relationId);
        if (relation == null) {
            Asserts.fail("秒杀商品不存在");
        }
        //场次时间窗校验：key 未过期但已不在场次时间内时不允许下单
        if (!insideSessionWindow(relation)) {
            LOGGER.warn("秒杀不在场次时间内，relationId:{}, 场次:{}~{}", relationId, relation.getSessionStartTime(), relation.getSessionEndTime());
            return CommonResult.failed("秒杀未开始或已结束，秒杀失败");
        }
        //限购兜底：单次购买量不超过 flash_promotion_limit
        int limit = relation.getFlashPromotionLimit() == null ? 0 : relation.getFlashPromotionLimit();
        final int quantity = (limit > 0 && requestQuantity > limit) ? limit : requestQuantity;
        long ttlSeconds = resolveTtlSeconds(relation);
        //2.Redis 原子预扣
        Long luaResult = executeSeckillLua(relationId, memberId, quantity, ttlSeconds);
        if (luaResult != null && luaResult == -1L) {
            //未预热（场次刚开始、应用刚启动、key 刚过期）：懒加载预热一次后重试
            LOGGER.warn("秒杀库存未预热，relationId:{} 触发懒加载预热", relationId);
            preheatRelation(relation);
            luaResult = executeSeckillLua(relationId, memberId, quantity, ttlSeconds);
        }
        if (luaResult == null) {
            return CommonResult.failed("秒杀服务暂不可用，请稍后重试");
        }
        if (luaResult == -1L) {
            return CommonResult.failed("秒杀未开始");
        }
        if (luaResult == -2L) {
            return CommonResult.failed("库存不足，秒杀失败");
        }
        if (luaResult == -3L) {
            return CommonResult.failed("您已参与过本次秒杀");
        }
        LOGGER.info("秒杀Redis预扣成功，relationId:{}, memberId:{}, quantity:{}, 剩余:{}", relationId, memberId, quantity, luaResult);
        //3.异步建单（v2 默认）：短事务落排队记录(status=0) + 事务提交后投递MQ消息，接口立即返回「排队中」
        if (asyncCreate) {
            return enqueueFlashOrder(relation, member, quantity);
        }
        //3.同步建单（v1，mall.flash.async-create=false）：编程式事务：唯一键冲突/库存扣不动都要回滚订单写入，并由外层补偿 Redis
        try {
            FlashOrderResult result = transactionTemplate.execute(status -> doCreateFlashOrder(relation, member, quantity));
            return CommonResult.success(result);
        } catch (DuplicateKeyException e) {
            //uk_relation_member 唯一键冲突：该会员已抢过（并发下唯一键是最终防线）
            compensateRedis(relationId, memberId, quantity);
            LOGGER.warn("秒杀重复下单被唯一键拦截，relationId:{}, memberId:{}", relationId, memberId);
            return CommonResult.failed("您已参与过本次秒杀");
        } catch (Exception e) {
            //4.落库失败：归还 Redis 名额（库存加回 + 移除已购标记），避免「Redis扣了但没订单」的名额黑洞
            compensateRedis(relationId, memberId, quantity);
            LOGGER.error("秒杀下单落库失败已补偿Redis，relationId:{}, memberId:{}, quantity:{}", relationId, memberId, quantity, e);
            throw e;
        }
    }

    /**
     * 异步建单第一步（v2）：短事务落一条 status=0（排队中）的成交记录（占住 uk_relation_member 唯一键），
     * 事务提交后投递建单消息，接口立即返回 status=0/排队中。
     * <p>
     * 投递失败不影响接口返回：记录已经是排队中，兜底扫描（FlashOrderCreateSweeperTask）会按 15 秒超时重投。
     */
    private CommonResult<FlashOrderResult> enqueueFlashOrder(FlashPromotionRelationCache relation, UmsMember member, int quantity) {
        Long relationId = relation.getRelationId();
        Long memberId = member.getId();
        Long productSkuId = resolveProductSkuId(relation);
        FlashPromotionRecord record;
        try {
            record = transactionTemplate.execute(status -> {
                FlashPromotionRecord queued = new FlashPromotionRecord();
                queued.setRelationId(relationId);
                queued.setMemberId(memberId);
                queued.setProductId(relation.getProductId());
                queued.setProductSkuId(productSkuId);
                queued.setQuantity(quantity);
                queued.setStatus(STATUS_QUEUED);
                flashPromotionRecordDao.insert(queued);
                return queued;
            });
        } catch (DuplicateKeyException e) {
            //uk_relation_member 唯一键冲突：两种可能——并发重复下单，或「上次建单失败(3)后重新抢」。
            //后者失败补偿时名额已还回 Redis，这里把同一条记录 CAS 回排队中并重投消息（不新增记录，唯一键依旧只有一条）
            FlashPromotionRecord existing = flashPromotionRecordDao.selectByRelationIdAndMemberId(relationId, memberId);
            if (existing != null && Integer.valueOf(STATUS_CREATE_FAILED).equals(existing.getStatus())
                    && flashPromotionRecordDao.resetToQueued(existing.getId()) == 1) {
                existing.setStatus(STATUS_QUEUED);
                flashOrderCreateSender.sendMessage(existing.getId());
                LOGGER.info("秒杀建单失败记录重新排队，relationId:{}, memberId:{}, requestId:{}, retryCount:{}",
                        relationId, memberId, existing.getId(), existing.getRetryCount());
                int retryQuantity = (existing.getQuantity() == null || existing.getQuantity() < 1) ? quantity : existing.getQuantity();
                return CommonResult.success(buildQueuedResult(relation, existing, retryQuantity));
            }
            //并发下唯一键是最终防线
            compensateRedis(relationId, memberId, quantity);
            LOGGER.warn("秒杀排队记录被唯一键拦截，relationId:{}, memberId:{}", relationId, memberId);
            return CommonResult.failed("您已参与过本次秒杀");
        } catch (Exception e) {
            compensateRedis(relationId, memberId, quantity);
            LOGGER.error("秒杀排队记录落库失败已补偿Redis，relationId:{}, memberId:{}", relationId, memberId, e);
            throw e;
        }
        if (record == null || record.getId() == null) {
            compensateRedis(relationId, memberId, quantity);
            LOGGER.error("秒杀排队记录落库未返回主键，relationId:{}, memberId:{}", relationId, memberId);
            return CommonResult.failed("秒杀服务暂不可用，请稍后重试");
        }
        //事务已提交后再投递，消费者一定能读到 status=0 的记录
        flashOrderCreateSender.sendMessage(record.getId());
        FlashOrderResult result = buildQueuedResult(relation, record, quantity);
        LOGGER.info("秒杀排队成功，relationId:{}, memberId:{}, quantity:{}, requestId:{}",
                relationId, memberId, quantity, record.getId());
        return CommonResult.success(result);
    }

    /**
     * 「排队中」结果的统一构造（首次排队与「建单失败后重新抢」共用）
     */
    private FlashOrderResult buildQueuedResult(FlashPromotionRelationCache relation, FlashPromotionRecord record, int quantity) {
        FlashOrderResult result = new FlashOrderResult();
        result.setRequestId(record.getId());
        result.setRelationId(record.getRelationId());
        result.setMemberId(record.getMemberId());
        result.setProductId(record.getProductId());
        result.setProductSkuId(record.getProductSkuId());
        result.setFlashPromotionPrice(relation.getFlashPromotionPrice());
        result.setQuantity(quantity);
        result.setStatus(STATUS_QUEUED);
        result.setStatusDesc(recordStatusDesc(STATUS_QUEUED));
        result.setCreateTime(record.getCreateTime() == null ? new Date() : record.getCreateTime());
        return result;
    }

    /**
     * 秒杀订单同步落库（v1 路径，mall.flash.async-create=false 时使用）：
     * 1) 插秒杀成交记录（uk_relation_member 冲突直接抛 DuplicateKeyException）；
     * 2)~4) 复用 {@link #createOrderForRecord}。
     */
    private FlashOrderResult doCreateFlashOrder(FlashPromotionRelationCache relation, UmsMember member, int quantity) {
        Long productSkuId = resolveProductSkuId(relation);
        //1)秒杀成交记录占位（无论后面是否成功都要先占住「一人一场次一单」的唯一键）
        FlashPromotionRecord record = new FlashPromotionRecord();
        record.setRelationId(relation.getRelationId());
        record.setMemberId(member.getId());
        record.setProductId(relation.getProductId());
        record.setProductSkuId(productSkuId);
        record.setQuantity(quantity);
        //0->排队中；建单成功后回写为 1(待支付)
        record.setStatus(STATUS_QUEUED);
        flashPromotionRecordDao.insert(record);
        return createOrderForRecord(record, relation, member, quantity);
    }

    /**
     * 按已存在的成交记录建单（同步模式与 MQ 异步消费者共用，必须在数据库事务内调用）：
     * 2) 数据库原子预扣 pms_sku_stock.lock_stock（扣不动 → 抛异常整单回滚）；
     * 3) 插 oms_order(order_type=1)/oms_order_item，回写成交记录的 order_id/order_sn 且 status=1(待支付)；
     * 4) 注册事务提交后回调，发送秒杀专用延迟取消消息（延迟 = flash_order_overtime 分钟）。
     */
    private FlashOrderResult createOrderForRecord(FlashPromotionRecord record, FlashPromotionRelationCache relation,
                                                  UmsMember member, int quantity) {
        Long memberId = member.getId();
        Long productSkuId = resolveProductSkuId(relation);
        PmsProduct product = pmsProductMapper.selectByPrimaryKey(relation.getProductId());
        PmsSkuStock skuStock = pmsSkuStockMapper.selectByPrimaryKey(productSkuId);
        BigDecimal flashPrice = relation.getFlashPromotionPrice();
        if (flashPrice == null) {
            Asserts.fail("秒杀价格未配置，无法下单");
        }
        //2)订单商品行（价格全部取服务端秒杀价，绝不使用前端传值）
        OmsOrderItem orderItem = new OmsOrderItem();
        orderItem.setProductId(relation.getProductId());
        orderItem.setProductSkuId(productSkuId);
        if (product != null) {
            orderItem.setProductName(product.getName());
            orderItem.setProductPic(product.getPic());
            orderItem.setProductSn(product.getProductSn());
            orderItem.setProductCategoryId(product.getProductCategoryId());
        }
        if (skuStock != null) {
            orderItem.setProductSkuCode(skuStock.getSkuCode());
            orderItem.setProductAttr(skuStock.getSpData());
        }
        BigDecimal realAmount = flashPrice.multiply(new BigDecimal(quantity));
        orderItem.setProductPrice(flashPrice);
        orderItem.setProductQuantity(quantity);
        orderItem.setPromotionName(StrUtil.isBlank(relation.getFlashPromotionTitle()) ? "秒杀活动" : "秒杀：" + relation.getFlashPromotionTitle());
        //秒杀价本身已是优惠价，不再叠加促销/优惠券/积分抵扣，保证 应付金额 = 秒杀价 × 数量
        orderItem.setPromotionAmount(BigDecimal.ZERO);
        orderItem.setCouponAmount(BigDecimal.ZERO);
        orderItem.setIntegrationAmount(BigDecimal.ZERO);
        orderItem.setRealAmount(realAmount);
        orderItem.setGiftIntegration(0);
        orderItem.setGiftGrowth(0);
        List<OmsOrderItem> orderItemList = new ArrayList<>();
        orderItemList.add(orderItem);
        //3)数据库侧原子预扣库存：影响行数不足说明 pms_sku_stock 可售库存不够，抛异常回滚整个事务
        int locked = portalOrderDao.lockSkuStock(orderItemList);
        if (locked < orderItemList.size()) {
            Asserts.fail("库存不足，无法下单");
        }
        //4)订单主表
        OmsOrder order = new OmsOrder();
        order.setMemberId(memberId);
        order.setMemberUsername(member.getUsername());
        order.setOrderSn(generateFlashOrderSn());
        //订单来源：1->app订单（与普通订单保持一致）
        order.setSourceType(FLASH_ORDER_SOURCE_TYPE);
        order.setPayType(FLASH_ORDER_PAY_TYPE);
        //订单状态：0->待付款
        order.setStatus(0);
        //订单类型：1->秒杀订单
        order.setOrderType(1);
        order.setTotalAmount(realAmount);
        order.setPayAmount(realAmount);
        order.setFreightAmount(BigDecimal.ZERO);
        order.setPromotionAmount(BigDecimal.ZERO);
        order.setCouponAmount(BigDecimal.ZERO);
        order.setIntegrationAmount(BigDecimal.ZERO);
        order.setDiscountAmount(BigDecimal.ZERO);
        //秒杀订单不赠送积分/成长值（v1 简化：秒杀价已是优惠价）
        order.setIntegration(0);
        order.setGrowth(0);
        order.setConfirmStatus(0);
        order.setDeleteStatus(0);
        order.setCreateTime(new Date());
        //收货人信息：秒杀下单页不选地址，取会员默认收货地址（没有地址时留空，不阻断抢购）
        UmsMemberReceiveAddress address = getDefaultAddress(memberId);
        if (address != null) {
            order.setReceiverName(address.getName());
            order.setReceiverPhone(address.getPhoneNumber());
            order.setReceiverPostCode(address.getPostCode());
            order.setReceiverProvince(address.getProvince());
            order.setReceiverCity(address.getCity());
            order.setReceiverRegion(address.getRegion());
            order.setReceiverDetailAddress(address.getDetailAddress());
        }
        //设置自动收货天数 + 取秒杀订单超时分钟数
        int flashOrderOvertime = DEFAULT_FLASH_ORDER_OVERTIME_MINUTES;
        List<OmsOrderSetting> orderSettings = omsOrderSettingMapper.selectByExample(new OmsOrderSettingExample());
        if (CollUtil.isNotEmpty(orderSettings)) {
            OmsOrderSetting setting = orderSettings.get(0);
            order.setAutoConfirmDay(setting.getConfirmOvertime());
            if (setting.getFlashOrderOvertime() != null) {
                flashOrderOvertime = setting.getFlashOrderOvertime();
            }
        }
        omsOrderMapper.insert(order);
        //5)订单商品行 + 回写成交记录
        orderItem.setOrderId(order.getId());
        orderItem.setOrderSn(order.getOrderSn());
        portalOrderItemDao.insertList(orderItemList);
        flashPromotionRecordDao.updateOrderInfoWithStatus(record.getId(), order.getId(), order.getOrderSn(), STATUS_UNPAID);
        //6)事务提交后再发延迟消息（避免消费者先于事务提交看不到订单）；
        //  MQ发送失败不能让「订单已落库」的接口变成报错，交给超时定时任务兜底取消
        final long delayTimes = flashOrderOvertime * 60L * 1000L;
        final Long orderId = order.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    flashOrderCancelSender.sendMessage(orderId, delayTimes);
                } catch (Exception e) {
                    LOGGER.error("发送秒杀订单延迟取消消息失败，orderId={}，将由超时定时任务兜底取消", orderId, e);
                }
            }
        });
        return buildFlashOrderResult(relation, record, order, productSkuId, flashPrice, quantity);
    }

    @Override
    public FlashOrderResult getFlashOrderResult(Long relationId) {
        if (relationId == null) {
            Asserts.fail("秒杀关系ID不能为空");
        }
        Long memberId = umsMemberService.getCurrentMember().getId();
        FlashPromotionRecord record = flashPromotionRecordDao.selectByRelationIdAndMemberId(relationId, memberId);
        if (record == null) {
            //没抢到：返回 success(null)，由前端展示「未参与/未抢到」
            return null;
        }
        FlashOrderResult result = new FlashOrderResult();
        result.setRelationId(relationId);
        result.setMemberId(memberId);
        result.setProductId(record.getProductId());
        result.setProductSkuId(record.getProductSkuId());
        result.setQuantity(record.getQuantity());
        result.setRequestId(record.getId());
        result.setStatus(record.getStatus());
        result.setStatusDesc(recordStatusDesc(record.getStatus()));
        result.setErrorMsg(record.getErrorMsg());
        result.setCreateTime(record.getCreateTime());
        result.setOrderId(record.getOrderId());
        result.setOrderSn(record.getOrderSn());
        if (record.getOrderId() != null) {
            OmsOrder order = omsOrderMapper.selectByPrimaryKey(record.getOrderId());
            if (order != null) {
                result.setOrderStatus(order.getStatus());
                //成交记录表未存价格，用「订单实付金额 ÷ 数量」还原单价（数量为1时即秒杀价）
                Integer quantity = record.getQuantity();
                if (order.getPayAmount() != null && quantity != null && quantity > 0) {
                    result.setFlashPromotionPrice(order.getPayAmount().divide(new BigDecimal(quantity), 2, RoundingMode.HALF_UP));
                }
            }
        }
        return result;
    }

    @Override
    public void createFlashOrderFromRecord(Long recordId) {
        if (recordId == null) {
            return;
        }
        //1.CAS 领取：只有排队中(0)能进入建单中(10)；影响行数0 = 已处理/重复消息/已被兜底判失败，直接幂等返回
        int claimed = flashPromotionRecordDao.updateStatusById(recordId, STATUS_QUEUED, STATUS_CREATING);
        if (claimed == 0) {
            LOGGER.info("秒杀建单消息幂等跳过（记录已不是排队中），recordId:{}", recordId);
            return;
        }
        FlashPromotionRecord record = flashPromotionRecordDao.selectById(recordId);
        if (record == null) {
            LOGGER.error("秒杀建单记录不存在，recordId:{}", recordId);
            return;
        }
        int quantity = (record.getQuantity() == null || record.getQuantity() < 1) ? 1 : record.getQuantity();
        try {
            //2.关系/会员/价格全部回源服务端数据，事务内锁库存+建单+回写记录(1 待支付)
            FlashPromotionRelationCache relation = getRelationCache(record.getRelationId());
            if (relation == null) {
                throw new IllegalStateException("秒杀关系不存在，relationId=" + record.getRelationId());
            }
            UmsMember member = umsMemberService.getById(record.getMemberId());
            if (member == null) {
                throw new IllegalStateException("会员不存在，memberId=" + record.getMemberId());
            }
            FlashOrderResult created = transactionTemplate.execute(status -> createOrderForRecord(record, relation, member, quantity));
            LOGGER.info("秒杀异步建单成功，recordId:{}, orderId:{}, orderSn:{}", recordId,
                    created == null ? null : created.getOrderId(), created == null ? null : created.getOrderSn());
        } catch (Exception e) {
            //3.失败补偿：事务已回滚 → 归还 Redis 名额（库存加回 + 移出已购集合）→ 置 3(建单失败) + 失败原因。
            //  不向外抛异常：否则消息会被反复重投，而缺库存这类失败重投多少次都不会成功。
            compensateRedis(record.getRelationId(), record.getMemberId(), quantity);
            String errorMsg = toErrorMsg(e);
            flashPromotionRecordDao.updateStatusAndError(recordId, STATUS_CREATE_FAILED, errorMsg, STATUS_CREATING);
            LOGGER.error("秒杀异步建单失败，已补偿Redis并置为建单失败(3)，recordId:{}, relationId:{}, memberId:{}, error:{}",
                    recordId, record.getRelationId(), record.getMemberId(), errorMsg, e);
        }
    }

    @Override
    public int resendQueuedFlashOrders() {
        Date before = new Date(System.currentTimeMillis() - SWEEP_RESEND_SECONDS * 1000L);
        List<Long> ids = flashPromotionRecordDao.selectIdsByStatusAndCreateTimeBefore(STATUS_QUEUED, before, SWEEP_BATCH_SIZE);
        if (CollUtil.isEmpty(ids)) {
            return 0;
        }
        int sent = 0;
        for (Long id : ids) {
            if (flashOrderCreateSender.sendMessage(id)) {
                sent++;
            }
        }
        LOGGER.warn("秒杀建单兜底扫描：查询到{}条排队超过{}秒的记录，重投{}条",
                ids.size(), SWEEP_RESEND_SECONDS, sent);
        return sent;
    }

    @Override
    public int failTimeoutQueuedFlashOrders() {
        Date before = new Date(System.currentTimeMillis() - SWEEP_TIMEOUT_MINUTES * 60L * 1000L);
        List<Long> ids = flashPromotionRecordDao.selectIdsByStatusAndCreateTimeBefore(STATUS_QUEUED, before, SWEEP_BATCH_SIZE);
        int failed = 0;
        for (Long id : ids) {
            FlashPromotionRecord record = flashPromotionRecordDao.selectById(id);
            if (record == null) {
                continue;
            }
            //先 CAS 抢到改写权再归还名额，保证「重投消息 + 兜底判失败」并发时只补偿一次
            if (flashPromotionRecordDao.updateStatusById(id, STATUS_QUEUED, STATUS_CREATING) == 0) {
                continue;
            }
            int quantity = (record.getQuantity() == null || record.getQuantity() < 1) ? 1 : record.getQuantity();
            compensateRedis(record.getRelationId(), record.getMemberId(), quantity);
            flashPromotionRecordDao.updateStatusAndError(id, STATUS_CREATE_FAILED,
                    "排队超过" + SWEEP_TIMEOUT_MINUTES + "分钟仍未建单，兜底判失败并归还名额", STATUS_CREATING);
            failed++;
            LOGGER.error("秒杀建单兜底扫描：recordId:{} 排队超过{}分钟仍未建单，已置为建单失败(3)并归还Redis名额",
                    id, SWEEP_TIMEOUT_MINUTES);
        }
        return failed;
    }

    @Override
    public boolean handleFlashOrderCreateDead(Long recordId) {
        if (recordId == null) {
            return false;
        }
        FlashPromotionRecord record = flashPromotionRecordDao.selectById(recordId);
        if (record == null || record.getStatus() == null) {
            return false;
        }
        Integer fromStatus = record.getStatus();
        if (fromStatus != STATUS_QUEUED && fromStatus != STATUS_CREATING) {
            LOGGER.info("秒杀死信记录当前状态为{}，无需处理，recordId:{}", fromStatus, recordId);
            return false;
        }
        int updated = flashPromotionRecordDao.updateStatusAndError(recordId, STATUS_CREATE_FAILED,
                "建单消息进入死信队列，兜底置为建单失败并归还名额", fromStatus);
        if (updated == 0) {
            return false;
        }
        int quantity = (record.getQuantity() == null || record.getQuantity() < 1) ? 1 : record.getQuantity();
        compensateRedis(record.getRelationId(), record.getMemberId(), quantity);
        LOGGER.error("秒杀建单死信兜底：recordId:{}, relationId:{}, memberId:{} 已置为建单失败(3)并归还名额",
                recordId, record.getRelationId(), record.getMemberId());
        return true;
    }

    /**
     * 异常信息截断（error_msg 列 varchar(255)）
     */
    private String toErrorMsg(Throwable e) {
        String msg = e.getMessage();
        if (StrUtil.isBlank(msg)) {
            msg = e.getClass().getSimpleName();
        }
        msg = msg.trim();
        return msg.length() > 200 ? msg.substring(0, 200) : msg;
    }

    @Override
    public List<FlashPromotionReconcileItem> reconcile() {
        List<FlashPromotionReconcileItem> items = new ArrayList<>();
        List<FlashPromotionRelationCache> relations = flashPromotionRecordDao.selectActiveRelations(DateUtil.getDate(new Date()), currentTimeString());
        if (CollUtil.isEmpty(relations)) {
            return items;
        }
        for (FlashPromotionRelationCache relation : relations) {
            Long relationId = relation.getRelationId();
            String stock = stringRedisTemplate.opsForValue().get(stockKey(relationId));
            Integer redisStock = stock == null ? null : Integer.valueOf(stock);
            Long buyers = stringRedisTemplate.opsForSet().size(buyersKey(relationId));
            Integer redisBuyers = buyers == null ? 0 : buyers.intValue();
            int total = relation.getFlashPromotionCount() == null ? 0 : relation.getFlashPromotionCount();
            Integer sold = flashPromotionRecordDao.sumQuantityByRelationId(relationId);
            Integer dbStock = Math.max(total - (sold == null ? 0 : sold), 0);
            Integer dbBuyers = flashPromotionRecordDao.countByRelationId(relationId);
            items.add(FlashPromotionReconcileItem.of(relationId, redisStock, dbStock, redisBuyers, dbBuyers));
        }
        return items;
    }

    @Override
    public boolean handleFlashOrderCancel(Long orderId) {
        if (orderId == null) {
            return false;
        }
        FlashPromotionRecord record = flashPromotionRecordDao.selectByOrderId(orderId);
        if (record == null) {
            LOGGER.warn("秒杀取消消息对应的成交记录不存在，orderId:{}", orderId);
            return false;
        }
        if (record.getStatus() != null && record.getStatus() == 2) {
            LOGGER.info("秒杀成交记录已是已取消状态，幂等返回，orderId:{}", orderId);
            return true;
        }
        OmsOrder order = omsOrderMapper.selectByPrimaryKey(orderId);
        if (order == null) {
            LOGGER.warn("秒杀取消消息对应的订单不存在，orderId:{}", orderId);
            return false;
        }
        if (order.getStatus() != null && order.getStatus() == 0) {
            //复用普通订单的取消逻辑：内部是 status=0 → 4 的 CAS 幂等更新，并释放 pms_sku_stock 的锁定库存
            portalOrderService.cancelOrder(orderId);
        }
        OmsOrder latest = omsOrderMapper.selectByPrimaryKey(orderId);
        if (latest == null || latest.getStatus() == null || latest.getStatus() != 4) {
            //已支付/已发货/已完成：不能归还秒杀名额
            LOGGER.info("秒杀订单当前状态为{}，不归还秒杀库存，orderId:{}", latest == null ? null : latest.getStatus(), orderId);
            return false;
        }
        //把记录从「非2」改成「2」只会有一次成功，以此保证 Redis 归还的幂等（重复投递不会重复加库存）
        int updated = flashPromotionRecordDao.updateStatusByOrderId(orderId, 2);
        if (updated == 1) {
            //归还 Redis 名额：库存加回 + 把该会员移出已购集合。
            //产品选择：取消后「允许该会员重新抢」（Redis 层面），已取消的记录不再占用秒杀名额。
            compensateRedis(record.getRelationId(), record.getMemberId(), record.getQuantity() == null ? 1 : record.getQuantity());
            LOGGER.info("秒杀订单取消成功并归还秒杀名额，orderId:{}, relationId:{}, memberId:{}", orderId, record.getRelationId(), record.getMemberId());
        } else {
            LOGGER.info("秒杀成交记录状态此前已更新，跳过归还，orderId:{}", orderId);
        }
        return true;
    }

    /**
     * 秒杀 Redis 预扣（Lua 原子脚本）
     *
     * @return 脚本返回值：>=0 扣减后剩余库存；-1 未预热；-2 库存不足；-3 重复下单；null Redis 不可用/脚本异常
     */
    private Long executeSeckillLua(Long relationId, Long memberId, int quantity, long ttlSeconds) {
        List<String> keys = Arrays.asList(stockKey(relationId), buyersKey(relationId));
        try {
            return stringRedisTemplate.execute(seckillScript, keys, String.valueOf(memberId), String.valueOf(quantity), String.valueOf(ttlSeconds));
        } catch (Exception e) {
            LOGGER.error("秒杀Lua脚本执行失败，relationId:{}, memberId:{}", relationId, memberId, e);
            return null;
        }
    }

    /**
     * 归还 Redis 名额（库存加回 + 移除已购标记）
     * <p>
     * 幂等：只在「确实扣减成功之后又失败」的分支里调用一次；库存key已过期时不做 incrby，
     * 避免把过期的库存key重新创建成一个错误的数值（此时下次预热会按数据库口径重建）。
     */
    private void compensateRedis(Long relationId, Long memberId, int quantity) {
        try {
            String stockKey = stockKey(relationId);
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(stockKey))) {
                stringRedisTemplate.opsForValue().increment(stockKey, quantity);
            }
            stringRedisTemplate.opsForSet().remove(buyersKey(relationId), String.valueOf(memberId));
            LOGGER.info("秒杀Redis补偿完成，relationId:{}, memberId:{}, quantity:{}", relationId, memberId, quantity);
        } catch (Exception e) {
            LOGGER.error("秒杀Redis补偿失败（不影响数据库结果，等待对账任务发现），relationId:{}, memberId:{}", relationId, memberId, e);
        }
    }

    /**
     * 取秒杀关系缓存，未命中回源数据库并预热回填
     */
    private FlashPromotionRelationCache getRelationCache(Long relationId) {
        String json = stringRedisTemplate.opsForValue().get(relationKey(relationId));
        if (StrUtil.isNotBlank(json)) {
            FlashPromotionRelationCache cache = fromJson(json);
            if (cache != null) {
                return cache;
            }
        }
        FlashPromotionRelationCache relation = flashPromotionRecordDao.selectRelationById(relationId);
        if (relation == null) {
            return null;
        }
        //回源后顺带把库存/买家集合/关系缓存一起预热，避免后续请求继续穿透
        preheatRelation(relation);
        return relation;
    }

    /**
     * 取秒杀商品SKU：优先用关系表的 product_sku_id；测试数据未配置时兜底取该商品「可售库存最多」的SKU，
     * 并打 WARN 日志（v1 不修改业务数据，只在下单时兜底，避免因为数据缺字段直接失败）
     */
    private Long resolveProductSkuId(FlashPromotionRelationCache relation) {
        if (relation.getProductSkuId() != null) {
            return relation.getProductSkuId();
        }
        PmsSkuStockExample example = new PmsSkuStockExample();
        example.createCriteria().andProductIdEqualTo(relation.getProductId());
        List<PmsSkuStock> skuStockList = pmsSkuStockMapper.selectByExample(example);
        if (CollUtil.isEmpty(skuStockList)) {
            Asserts.fail("秒杀商品未配置SKU，无法下单");
        }
        PmsSkuStock candidate = skuStockList.stream()
                .max(Comparator.comparingInt((PmsSkuStock sku) -> {
                    int stock = sku.getStock() == null ? 0 : sku.getStock();
                    int lockStock = sku.getLockStock() == null ? 0 : sku.getLockStock();
                    return stock - lockStock;
                }))
                .orElse(skuStockList.get(0));
        LOGGER.warn("秒杀关系{}未配置product_sku_id，兜底使用商品{}可售库存最多的SKU:{}",
                relation.getRelationId(), relation.getProductId(), candidate.getId());
        return candidate.getId();
    }

    /**
     * 生成秒杀订单号：与普通订单共用同一个自增序列（key: mall:oms:orderId{yyyyMMdd}）+ 同一套编码规则
     */
    private String generateFlashOrderSn() {
        StringBuilder sb = new StringBuilder();
        String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String key = redisDatabase + ":" + redisKeyOrderId + date;
        Long orderId = stringRedisTemplate.opsForValue().increment(key);
        sb.append(date).append(String.format("%02d", FLASH_ORDER_SOURCE_TYPE))
                .append(String.format("%02d", FLASH_ORDER_PAY_TYPE))
                .append(String.format("%06d", orderId == null ? 0L : orderId));
        return sb.toString();
    }

    /**
     * 会员默认收货地址（没有默认地址时取第一条；完全没有地址返回 null）
     * 注意：MQ 消费者线程没有 SecurityContext，不能再用 memberReceiveAddressService.list()（内部依赖当前登录会员），
     * 统一按 memberId 直接查库，保证同步/异步两条链路行为一致。
     */
    private UmsMemberReceiveAddress getDefaultAddress(Long memberId) {
        if (memberId == null) {
            return null;
        }
        UmsMemberReceiveAddressExample example = new UmsMemberReceiveAddressExample();
        example.createCriteria().andMemberIdEqualTo(memberId);
        example.setOrderByClause("default_status desc, id asc");
        List<UmsMemberReceiveAddress> addressList = memberReceiveAddressMapper.selectByExample(example);
        if (CollUtil.isEmpty(addressList)) {
            return null;
        }
        return addressList.stream()
                .filter(address -> Integer.valueOf(1).equals(address.getDefaultStatus()))
                .findFirst()
                .orElse(addressList.get(0));
    }

    /**
     * 判断当前时间是否落在场次时间段内（支持跨零点场次；场次时间缺失时不拦截，避免误伤测试数据）
     */
    private boolean insideSessionWindow(FlashPromotionRelationCache relation) {
        LocalTime start = parseTime(relation.getSessionStartTime());
        LocalTime end = parseTime(relation.getSessionEndTime());
        if (start == null || end == null) {
            return true;
        }
        LocalTime now = LocalTime.now();
        if (!start.isAfter(end)) {
            //同一自然日内的场次（含 start == end）
            return !now.isBefore(start) && !now.isAfter(end);
        }
        //跨零点场次：如 22:00~02:00
        return !now.isBefore(start) || !now.isAfter(end);
    }

    /**
     * 计算库存/买家key的TTL（秒）：场次结束即到期，跨零点场次按次日结束时间计算
     */
    private long resolveTtlSeconds(FlashPromotionRelationCache relation) {
        LocalTime start = parseTime(relation.getSessionStartTime());
        LocalTime end = parseTime(relation.getSessionEndTime());
        if (end == null) {
            return MIN_KEY_TTL_SECONDS;
        }
        LocalTime now = LocalTime.now();
        Duration duration;
        if (start != null && start.isAfter(end) && !now.isAfter(end)) {
            //跨零点场次且当前处于后半夜：结束时间就是今天
            duration = Duration.between(now, end);
        } else if (start != null && start.isAfter(end)) {
            //跨零点场次且当前处于前半夜：结束时间是次日
            duration = Duration.between(now, end).plusDays(1);
        } else {
            duration = Duration.between(now, end);
        }
        return Math.max(duration.getSeconds(), MIN_KEY_TTL_SECONDS);
    }

    private LocalTime parseTime(String time) {
        if (StrUtil.isBlank(time)) {
            return null;
        }
        try {
            return LocalTime.parse(time.trim(), TIME_FORMATTER);
        } catch (Exception e) {
            LOGGER.warn("秒杀场次时间格式无法解析：{}", time);
            return null;
        }
    }

    /**
     * 当前时分秒（用于与 TIME 列比较，避免日期部分干扰）
     */
    private String currentTimeString() {
        return new SimpleDateFormat("HH:mm:ss").format(DateUtil.getTime(new Date()));
    }

    private FlashOrderResult buildFlashOrderResult(FlashPromotionRelationCache relation, FlashPromotionRecord record,
                                                   OmsOrder order, Long productSkuId, BigDecimal flashPrice, int quantity) {
        FlashOrderResult result = new FlashOrderResult();
        result.setRelationId(relation.getRelationId());
        result.setOrderId(order.getId());
        result.setOrderSn(order.getOrderSn());
        result.setMemberId(order.getMemberId());
        result.setProductId(relation.getProductId());
        result.setProductSkuId(productSkuId);
        result.setFlashPromotionPrice(flashPrice);
        result.setQuantity(quantity);
        result.setStatus(record.getStatus());
        result.setOrderStatus(order.getStatus());
        result.setStatusDesc(recordStatusDesc(record.getStatus()));
        result.setCreateTime(new Date());
        return result;
    }

    private String recordStatusDesc(Integer status) {
        if (status == null) {
            return "未知";
        }
        switch (status) {
            case STATUS_QUEUED:
                return "排队中";
            case STATUS_CREATING:
                return "建单中";
            case STATUS_UNPAID:
                return "待支付";
            case STATUS_CANCELED:
                return "已取消";
            case STATUS_CREATE_FAILED:
                return "建单失败";
            case STATUS_PAID:
                return "已支付";
            default:
                return "未知";
        }
    }

    private String stockKey(Long relationId) {
        return STOCK_KEY_PREFIX + relationId;
    }

    private String buyersKey(Long relationId) {
        return BUYERS_KEY_PREFIX + relationId;
    }

    private String relationKey(Long relationId) {
        return RELATION_KEY_PREFIX + relationId;
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOGGER.error("秒杀关系缓存序列化失败", e);
            return null;
        }
    }

    private FlashPromotionRelationCache fromJson(String json) {
        try {
            return objectMapper.readValue(json, FlashPromotionRelationCache.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("秒杀关系缓存反序列化失败，将回源数据库：{}", json, e);
            return null;
        }
    }
}