package com.example.booking.repository;

import com.example.booking.domain.BookingStateHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface BookingStateHistoryRepository extends JpaRepository<BookingStateHistory, UUID> {

    List<BookingStateHistory> findByBookingIdOrderByCreatedAtAsc(UUID bookingId);
}
