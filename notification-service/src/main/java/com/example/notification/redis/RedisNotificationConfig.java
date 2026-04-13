package com.example.notification.redis;

import com.example.notification.common.NotificationConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisNotificationConfig {

    @Bean
    RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            BookingStatusRedisFanoutService bookingStatusRedisFanoutService,
            @Value(NotificationConstants.Redis.BOOKING_STATUS_CHANNEL) String bookingStatusChannel
    ) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                (message, pattern) -> bookingStatusRedisFanoutService.onMessage(message),
                new ChannelTopic(bookingStatusChannel)
        );
        return container;
    }
}
