-- 秒杀 Redis 预扣脚本（v1）
-- KEYS[1]=库存key mall1:seckill:stock:{relationId}   KEYS[2]=已购会员set mall1:seckill:buyers:{relationId}
-- ARGV[1]=memberId   ARGV[2]=quantity   ARGV[3]=ttl秒
-- 返回值：>=0 扣减后的剩余库存；-1 未预热（key 不存在）；-2 库存不足；-3 重复下单
-- 说明：整个「查库存 -> 判重复 -> 扣减 -> 记买家」过程在 Redis 单线程内原子完成，
--       这是第一道防超卖闸门；数据库侧还有 sms_flash_promotion_record 唯一键与 pms_sku_stock 原子扣减两道闸门。
local stock = tonumber(redis.call('get', KEYS[1]))
if stock == nil then return -1 end                 -- 未预热
if stock < tonumber(ARGV[2]) then return -2 end    -- 库存不足
if redis.call('sismember', KEYS[2], ARGV[1]) == 1 then return -3 end  -- 重复下单
redis.call('decrby', KEYS[1], ARGV[2])
redis.call('sadd', KEYS[2], ARGV[1])
redis.call('expire', KEYS[2], ARGV[3])
return stock - tonumber(ARGV[2])