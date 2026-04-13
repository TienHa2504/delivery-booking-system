CREATE TABLE delivery_opportunity (
    opportunity_id UUID PRIMARY KEY,
    region_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    booking_window_start TIMESTAMPTZ NOT NULL,
    booking_window_end TIMESTAMPTZ NOT NULL,
    capacity INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_delivery_opportunity_booking_window CHECK (booking_window_start < booking_window_end),
    CONSTRAINT chk_delivery_opportunity_capacity CHECK (capacity >= 0)
);

CREATE INDEX idx_delivery_opportunity_region_zone
    ON delivery_opportunity (region_id, zone_id);

CREATE INDEX idx_delivery_opportunity_booking_window
    ON delivery_opportunity (booking_window_start, booking_window_end);

CREATE TABLE booking (
    booking_id UUID PRIMARY KEY,
    driver_id UUID NOT NULL,
    opportunity_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL,
    slot_reserved BOOLEAN NOT NULL DEFAULT false,
    slot_released BOOLEAN NOT NULL DEFAULT false,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error_code VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_booking_delivery_opportunity
        FOREIGN KEY (opportunity_id) REFERENCES delivery_opportunity (opportunity_id),
    CONSTRAINT uk_booking_opportunity_driver UNIQUE (opportunity_id, driver_id),
    CONSTRAINT chk_booking_status CHECK (status IN ('PENDING', 'PROCESSING', 'CONFIRMED', 'FAILED')),
    CONSTRAINT chk_booking_retry_count CHECK (retry_count >= 0),
    CONSTRAINT chk_booking_slot_release_requires_reservation CHECK (slot_released = false OR slot_reserved = true)
);

CREATE INDEX idx_booking_driver_id
    ON booking (driver_id);

CREATE INDEX idx_booking_opportunity_status
    ON booking (opportunity_id, status);

CREATE INDEX idx_booking_status_updated_at
    ON booking (status, updated_at);

CREATE TABLE retry_record (
    id UUID PRIMARY KEY,
    booking_id UUID NOT NULL,
    driver_id UUID NOT NULL,
    opportunity_id UUID NOT NULL,
    error_code VARCHAR(128) NOT NULL,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retry INTEGER NOT NULL,
    next_retry_at TIMESTAMPTZ NOT NULL,
    retry_status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_retry_record_booking
        FOREIGN KEY (booking_id) REFERENCES booking (booking_id),
    CONSTRAINT fk_retry_record_delivery_opportunity
        FOREIGN KEY (opportunity_id) REFERENCES delivery_opportunity (opportunity_id),
    CONSTRAINT chk_retry_record_retry_count CHECK (retry_count >= 0),
    CONSTRAINT chk_retry_record_max_retry CHECK (max_retry >= 0),
    CONSTRAINT chk_retry_record_retry_status CHECK (retry_status IN ('PENDING', 'PROCESSING', 'SUCCEEDED', 'FAILED', 'EXHAUSTED'))
);

CREATE INDEX idx_retry_record_status_next_retry_at
    ON retry_record (retry_status, next_retry_at);

CREATE INDEX idx_retry_record_booking_id
    ON retry_record (booking_id);

CREATE INDEX idx_retry_record_opportunity_driver
    ON retry_record (opportunity_id, driver_id);

CREATE TABLE booking_state_history (
    id UUID PRIMARY KEY,
    booking_id UUID NOT NULL,
    from_state VARCHAR(32),
    to_state VARCHAR(32) NOT NULL,
    reason VARCHAR(256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_booking_state_history_booking
        FOREIGN KEY (booking_id) REFERENCES booking (booking_id),
    CONSTRAINT chk_booking_state_history_from_state CHECK (from_state IS NULL OR from_state IN ('PENDING', 'PROCESSING', 'CONFIRMED', 'FAILED')),
    CONSTRAINT chk_booking_state_history_to_state CHECK (to_state IN ('PENDING', 'PROCESSING', 'CONFIRMED', 'FAILED'))
);

CREATE INDEX idx_booking_state_history_booking_created_at
    ON booking_state_history (booking_id, created_at);
