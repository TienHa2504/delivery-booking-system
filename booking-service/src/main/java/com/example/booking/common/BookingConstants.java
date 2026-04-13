package com.example.booking.common;

public final class BookingConstants {

    private BookingConstants() {
    }

    public static final class Kafka {

        public static final String BOOKING_CREATED_TOPIC = "${booking.kafka.topics.booking-created}";
        public static final String BOOKING_STATUS_TOPIC = "${booking.kafka.topics.booking-status}";
        public static final String BOOKING_DLQ_TOPIC = "${booking.kafka.topics.booking-dlq}";
        public static final String CONSUMER_GROUP_ID = "${spring.kafka.consumer.group-id}";
        public static final String DLQ_CONSUMER_GROUP_ID = "${spring.kafka.consumer.group-id}-dlq";
        public static final String LISTENER_CONTAINER_FACTORY = "bookingKafkaListenerContainerFactory";
        public static final String DLQ_LISTENER_CONTAINER_FACTORY = "bookingDlqKafkaListenerContainerFactory";

        private Kafka() {
        }
    }

    public static final class EventType {

        public static final String BOOKING_CREATED = "BOOKING_CREATED";
        public static final String BOOKING_STATUS_CHANGED = "BOOKING_STATUS_CHANGED";
        public static final String BOOKING_RETRY_REQUESTED = "BOOKING_RETRY_REQUESTED";
        public static final String BOOKING_FAILED_FINAL = "BOOKING_FAILED_FINAL";

        private EventType() {
        }
    }

    public static final class StateReason {

        public static final String BOOKING_REQUEST_ACCEPTED = "BOOKING_REQUEST_ACCEPTED";
        public static final String BOOKING_CREATED_CONSUMED = "BOOKING_CREATED_CONSUMED";
        public static final String BOOKING_PROCESSING_SUCCEEDED = "BOOKING_PROCESSING_SUCCEEDED";

        private StateReason() {
        }
    }

    public static final class ProcessingError {

        public static final String BOOKING_NOT_FOUND = "BOOKING_NOT_FOUND";
        public static final String BOOKING_PROCESSING_UNEXPECTED = "BOOKING_PROCESSING_UNEXPECTED";
        public static final String BOOKING_NOT_PROCESSING = "BOOKING_NOT_PROCESSING";
        public static final String SLOT_NOT_RESERVED = "SLOT_NOT_RESERVED";
        public static final String SLOT_ALREADY_RELEASED = "SLOT_ALREADY_RELEASED";

        private ProcessingError() {
        }
    }

    public static final class Retry {

        public static final String MAX_ATTEMPTS = "${booking.retry.max-attempts:3}";
        public static final String DELAY = "${booking.retry.delay:PT30S}";

        private Retry() {
        }
    }

    public static final class Reservation {

        public static final String TTL = "${booking.reservation.ttl}";

        private Reservation() {
        }
    }
}
