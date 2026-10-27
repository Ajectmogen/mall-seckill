package com.macro.mall.portal.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * 秒杀相关Redis配置
 * <p>
 * 只负责声明脚本Bean，<b>不做任何连接预热</b>：脚本对象在创建时只是把 lua 文本读进内存，
 * 真正的 Redis 连接在第一次执行脚本时才建立，因此 Redis 不可用时不会拖垮应用启动。
 * <p>
 * 注意：这里必须使用 Spring Boot 自带的 {@code StringRedisTemplate}（String 序列化）来执行本脚本，
 * 不能使用 mall-common 里的 {@code RedisService}（RedisTemplate&lt;String,Object&gt; + Jackson 值序列化），
 * 否则 Lua 的 ARGV 会被序列化成带引号的 JSON（如 "1"），脚本内 tonumber() 会直接报错。
 * Created by mall-1 seckill v1.
 */
@Configuration
public class SeckillRedisConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeckillRedisConfig.class);

    /**
     * 秒杀预扣脚本（classpath:lua/seckill.lua）
     */
    @Bean
    public DefaultRedisScript<Long> seckillScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setResultType(Long.class);
        ClassPathResource resource = new ClassPathResource("lua/seckill.lua");
        if (resource.exists()) {
            script.setLocation(resource);
        } else {
            //脚本文件缺失属于打包问题，这里只记日志、不阻断启动，运行时调用会返回"秒杀服务暂不可用"
            LOGGER.error("秒杀Lua脚本 lua/seckill.lua 不存在，秒杀下单将不可用");
        }
        return script;
    }
}