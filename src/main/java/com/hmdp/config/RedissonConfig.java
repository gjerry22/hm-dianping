package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ClassName: RedissonConfig
 * Package: com.hmdp.config
 * Description:
 *
 * @Author: Jerry
 * @Date: 2023/8/22
 */

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis:://222.28.66.77:6379").setPassword("redis");
        return Redisson.create(config);
    }

}
