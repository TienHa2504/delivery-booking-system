package com.example.batch.common;

public final class BatchConstants {

    private BatchConstants() {
    }

    public static final class Job {

        public static final String RETRY_JOB = "retryJob";
        public static final String RETRY_STEP = "retryStep";
        public static final String STATUS_RECONCILIATION_JOB = "statusReconciliationJob";
        public static final String STATUS_RECONCILIATION_STEP = "statusReconciliationStep";
        public static final String REMAINING_RECONCILIATION_JOB = "remainingReconciliationJob";
        public static final String REMAINING_RECONCILIATION_STEP = "remainingReconciliationStep";

        private Job() {
        }
    }

    public static final class Kafka {

        public static final String BOOKING_CREATED_TOPIC = "${booking.kafka.topics.booking-created}";
        public static final String BOOKING_STATUS_TOPIC = "${booking.kafka.topics.booking-status}";

        private Kafka() {
        }
    }

    public static final class EventType {

        public static final String BOOKING_CREATED = "BOOKING_CREATED";
        public static final String BOOKING_STATUS_CHANGED = "BOOKING_STATUS_CHANGED";

        private EventType() {
        }
    }

    public static final class StateReason {

        public static final String RETRY_JOB_CONFIRMED = "RETRY_JOB_CONFIRMED";
        public static final String RETRY_JOB_FAILED = "RETRY_JOB_FAILED";
        public static final String STALE_PENDING_RECONCILED = "STALE_PENDING_RECONCILED";
        public static final String STALE_PENDING_REDRIVE_REQUESTED = "STALE_PENDING_REDRIVE_REQUESTED";
        public static final String STALE_PENDING_BOOKING_WINDOW_EXPIRED = "STALE_PENDING_BOOKING_WINDOW_EXPIRED";
        public static final String STALE_PROCESSING_RETRY_EXHAUSTED = "STALE_PROCESSING_RETRY_EXHAUSTED";

        private StateReason() {
        }
    }

    public static final class Error {

        public static final String RETRY_PROCESSING_FAILED = "RETRY_PROCESSING_FAILED";
        public static final String RETRY_RECORD_NOT_PENDING = "RETRY_RECORD_NOT_PENDING";
        public static final String RETRY_BOOKING_MISMATCH = "RETRY_BOOKING_MISMATCH";
        public static final String RETRY_SLOT_NOT_RESERVED = "RETRY_SLOT_NOT_RESERVED";
        public static final String RETRY_SLOT_ALREADY_RELEASED = "RETRY_SLOT_ALREADY_RELEASED";
        public static final String RETRY_BOOKING_NOT_RETRYABLE = "RETRY_BOOKING_NOT_RETRYABLE";
        public static final String STALE_PENDING_TIMEOUT = "STALE_PENDING_TIMEOUT";
        public static final String STALE_PENDING_BOOKING_WINDOW_EXPIRED = "STALE_PENDING_BOOKING_WINDOW_EXPIRED";
        public static final String STALE_PENDING_BOOKING_CREATED_EVENT_NOT_PUBLISHED = "STALE_PENDING_BOOKING_CREATED_EVENT_NOT_PUBLISHED";
        public static final String STALE_PENDING_BOOKING_CREATED_EVENT_REDRIVE_FAILED = "STALE_PENDING_BOOKING_CREATED_EVENT_REDRIVE_FAILED";
        public static final String STALE_PROCESSING_TIMEOUT = "STALE_PROCESSING_TIMEOUT";
        public static final String ORPHAN_REDIS_RESERVATION = "ORPHAN_REDIS_RESERVATION";

        private Error() {
        }
    }

    public static final class Retry {

        public static final String PAGE_SIZE = "${booking.retry.page-size:100}";
        public static final String DELAY = "${booking.retry.delay:PT30S}";
        public static final String MAX_ATTEMPTS = "${booking.retry.max-attempts:3}";
        public static final String SCHEDULE_FIXED_DELAY = "${booking.retry.schedule.fixed-delay:PT30S}";
        public static final String SCHEDULE_INITIAL_DELAY = "${booking.retry.schedule.initial-delay:PT10S}";
        public static final String SCHEDULE_LOCK_AT_MOST_FOR = "${booking.retry.schedule.lock-at-most-for:PT5M}";
        public static final String SCHEDULE_LOCK_AT_LEAST_FOR = "${booking.retry.schedule.lock-at-least-for:PT1S}";

        private Retry() {
        }
    }

    public static final class BookingCreatedEventPublish {

        public static final String RETRY_DELAY = "${booking.booking-created-event-publish.retry-delay:PT10S}";

        private BookingCreatedEventPublish() {
        }
    }

    public static final class Reconciliation {

        public static final String PAGE_SIZE = "${booking.reconciliation.page-size:100}";
        public static final String PENDING_TIMEOUT = "${booking.reconciliation.pending-timeout:PT10M}";
        public static final String PROCESSING_TIMEOUT = "${booking.reconciliation.processing-timeout:PT10M}";
        public static final String SCHEDULE_FIXED_DELAY = "${booking.reconciliation.schedule.fixed-delay:PT1M}";
        public static final String SCHEDULE_INITIAL_DELAY = "${booking.reconciliation.schedule.initial-delay:PT20S}";
        public static final String SCHEDULE_LOCK_AT_MOST_FOR = "${booking.reconciliation.schedule.lock-at-most-for:PT10M}";
        public static final String SCHEDULE_LOCK_AT_LEAST_FOR = "${booking.reconciliation.schedule.lock-at-least-for:PT1S}";

        private Reconciliation() {
        }
    }

    public static final class RemainingReconciliation {

        public static final String PAGE_SIZE = "${booking.remaining-reconciliation.page-size:100}";
        public static final String GRACE_PERIOD = "${booking.remaining-reconciliation.grace-period:PT60S}";
        public static final String SCHEDULE_FIXED_DELAY = "${booking.remaining-reconciliation.schedule.fixed-delay:PT2M}";
        public static final String SCHEDULE_INITIAL_DELAY = "${booking.remaining-reconciliation.schedule.initial-delay:PT30S}";
        public static final String SCHEDULE_LOCK_AT_MOST_FOR = "${booking.remaining-reconciliation.schedule.lock-at-most-for:PT10M}";
        public static final String SCHEDULE_LOCK_AT_LEAST_FOR = "${booking.remaining-reconciliation.schedule.lock-at-least-for:PT1S}";

        private RemainingReconciliation() {
        }
    }

    public static final class Scheduler {

        public static final String LOCK_ENVIRONMENT = "${booking.scheduler.lock-environment:batch-service}";

        private Scheduler() {
        }
    }

}
