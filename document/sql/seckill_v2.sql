-- ============================================================================
-- 秒杀 v2（MQ 异步建单）DDL
-- 库：mall1    执行：mysql -uroot -p<password> --default-character-set=utf8mb4 -N -B mall1 < document/sql/seckill_v2.sql
-- 说明：本脚本只执行一次；重复执行会因列/索引已存在而报错。
-- 状态机：0 排队中(已扣Redis名额,消息未消费) -> 10 建单中(消费者CAS领取) -> 1 待支付
--         -> 2 已取消(超时/用户取消) / 4 已支付；3 建单失败(名额已还回Redis)
--         占用秒杀名额的口径统一为 status IN (0,10,1,4)
-- ============================================================================

ALTER TABLE sms_flash_promotion_record
    ADD COLUMN error_msg varchar(255) NULL COMMENT '建单失败原因（status=3时写入）',
    ADD COLUMN retry_count int NOT NULL DEFAULT 0 COMMENT '建单重试次数（失败补偿时+1）',
    MODIFY COLUMN status int NOT NULL DEFAULT 0 COMMENT '0排队中 10建单中 1待支付 2已取消 3建单失败 4已支付',
    ADD INDEX idx_status_ctime (status, create_time);