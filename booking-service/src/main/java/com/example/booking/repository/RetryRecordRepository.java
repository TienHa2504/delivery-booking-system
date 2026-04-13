package com.example.booking.repository;

import com.example.booking.domain.RetryRecord;
import com.example.booking.domain.RetryStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface RetryRecordRepository extends JpaRepository<RetryRecord, UUID> {

    List<RetryRecord> findByRetryStatusAndNextRetryAtLessThanEqual(RetryStatus retryStatus, Instant now);

    boolean existsByBookingIdAndRetryStatus(UUID bookingId, RetryStatus retryStatus);
}
