-- ============================================================
-- 秒杀（限时购）下单 v1 建表脚本
-- 执行方式：mysql -uroot -p<password> --default-character-set=utf8mb4 -N -B mall1 < document/sql/seckill.sql
-- 说明：
--   0) 必须带 --default-character-set=utf8mb4：Windows 版 mysql 客户端默认按 GBK 解释脚本文件字节，
--      否则本文件里的中文 COMMENT 会被双重编码成乱码；
--   1) 若报 "Duplicate column name 'product_sku_id'" 说明脚本已执行过，直接跳过即可；
--   2) sms_flash_promotion_record 是秒杀成交记录表，uk_relation_member 是
--      「同一场次同一商品同一会员只能抢一次」的数据库侧最终兜底（Redis 侧的 buyers set 只是前置拦截）；
--   3) order_id 上额外加了一个普通索引，用于「超时取消」消费者按 orderId 反查成交记录。
-- ============================================================

ALTER TABLE sms_flash_promotion_product_relation ADD COLUMN product_sku_id bigint(20) NULL COMMENT '秒杀商品SKU编号' AFTER product_id;

CREATE TABLE `sms_flash_promotion_record` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `relation_id` bigint(20) NOT NULL COMMENT '秒杀关系ID',
  `member_id` bigint(20) NOT NULL COMMENT '会员ID',
  `product_id` bigint(20) NULL,
  `product_sku_id` bigint(20) NULL,
  `order_id` bigint(20) NULL,
  `order_sn` varchar(64) NULL,
  `quantity` int(11) DEFAULT 1,
  `status` int(1) DEFAULT 0 COMMENT '0待支付 1已支付 2已取消',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_relation_member` (`relation_id`,`member_id`),
  UNIQUE KEY `uk_order_sn` (`order_sn`),
  KEY `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀成交记录表';