package com.example.notification.common;

public final class NotificationConstants {

    private NotificationConstants() {
    }

    public static final class Kafka {

        public static final String BOOKING_STATUS_TOPIC = "${notification.kafka.topics.booking-status}";
        public static final String CONSUMER_GROUP_ID = "${spring.kafka.consumer.group-id}";

        private Kafka() {
        }
    }

    public static final class Sse {

        public static final String TIMEOUT = "${notification.sse.timeout}";

        private Sse() {
        }
    }

    public static final class Redis {

        public static final String BOOKING_STATUS_CHANNEL = "${notification.redis.channels.booking-status}";

        private Redis() {
        }
    }
}
