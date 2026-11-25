package com.macro.mall.portal.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/** 秒杀 Lua 脚本 Bean；只加载脚本文本，不建立 Redis 连接 */
@Configuration
public class SeckillRedisConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeckillRedisConfig.class);

    /** 秒杀预扣脚本（classpath:lua/seckill.lua） */
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