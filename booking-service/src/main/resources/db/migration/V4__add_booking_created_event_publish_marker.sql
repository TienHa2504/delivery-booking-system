ALTER TABLE booking
    ADD COLUMN booking_created_event_published BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN booking_created_event_published_at TIMESTAMPTZ,
    ADD COLUMN booking_created_event_retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN booking_created_event_next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN booking_created_event_last_error TEXT,
    ADD CONSTRAINT chk_booking_created_event_retry_count CHECK (booking_created_event_retry_count >= 0);

CREATE INDEX idx_booking_created_event_publish_retry
    ON booking (booking_created_event_published, booking_created_event_next_retry_at)
    WHERE booking_created_event_published = false;
