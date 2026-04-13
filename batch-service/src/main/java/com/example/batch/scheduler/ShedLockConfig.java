package com.example.batch.scheduler;

import com.example.batch.common.BatchConstants;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.redis.spring.RedisLockProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Configuration
public class ShedLockConfig {

    @Bean
    LockProvider lockProvider(
            RedisConnectionFactory connectionFactory,
            @Value(BatchConstants.Scheduler.LOCK_ENVIRONMENT) String lockEnvironment
    ) {
        return new RedisLockProvider(connectionFactory, lockEnvironment);
    }
}
