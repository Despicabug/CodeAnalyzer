package com.codeanalyser.worker.redis;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Redis configuration. Declares a StringRedisTemplate so all keys and values
 * are stored as plain UTF-8 strings, readable via redis-cli without decoding.
 */
@Configuration
@EnableScheduling
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
