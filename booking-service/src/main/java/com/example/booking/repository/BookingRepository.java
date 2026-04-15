package com.example.booking.repository;

import com.example.booking.domain.Booking;
import com.example.booking.domain.BookingStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface BookingRepository extends JpaRepository<Booking, UUID> {

    Optional<Booking> findByOpportunityIdAndDriverId(UUID opportunityId, UUID driverId);

    long countByOpportunityIdAndStatusIn(UUID opportunityId, List<BookingStatus> statuses);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select b from Booking b where b.bookingId = :bookingId")
    Optional<Booking> findByIdForUpdate(UUID bookingId);

    @Modifying
    @Query("""
            update Booking b
            set b.bookingCreatedEventPublished = true,
                b.bookingCreatedEventPublishedAt = :publishedAt,
                b.bookingCreatedEventLastError = null
            where b.bookingId = :bookingId
            """)
    void markBookingCreatedEventPublished(UUID bookingId, Instant publishedAt);

    @Modifying
    @Query("""
            update Booking b
            set b.bookingCreatedEventPublished = false,
                b.bookingCreatedEventRetryCount = b.bookingCreatedEventRetryCount + 1,
                b.bookingCreatedEventNextRetryAt = :nextRetryAt,
                b.bookingCreatedEventLastError = :lastError
            where b.bookingId = :bookingId
            """)
    void markBookingCreatedEventPublishFailed(UUID bookingId, Instant nextRetryAt, String lastError);
}
