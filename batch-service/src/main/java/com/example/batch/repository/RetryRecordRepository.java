package com.example.batch.repository;

import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RetryRecordRepository extends JpaRepository<RetryRecord, UUID> {

    List<RetryRecord> findByRetryStatusAndNextRetryAtLessThanEqual(RetryStatus retryStatus, Instant now);

    boolean existsByBookingIdAndRetryStatusIn(UUID bookingId, List<RetryStatus> retryStatuses);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from RetryRecord r where r.id = :id")
    Optional<RetryRecord> findByIdForUpdate(UUID id);
}
