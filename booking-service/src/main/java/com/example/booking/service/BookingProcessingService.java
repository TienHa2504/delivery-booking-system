package com.example.booking.service;

import com.example.booking.common.BookingConstants;
import com.example.booking.domain.Booking;
import com.example.booking.domain.BookingStatus;
import com.example.booking.exception.NonRecoverableBookingProcessingException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class BookingProcessingService {

    @Async
    public CompletableFuture<Void> process(Booking booking) {
        if (booking.getStatus() != BookingStatus.PROCESSING) {
            throw new NonRecoverableBookingProcessingException(
                    BookingConstants.ProcessingError.BOOKING_NOT_PROCESSING,
                    "Booking must be PROCESSING before processing can run"
            );
        }
        if (!booking.isSlotReserved()) {
            throw new NonRecoverableBookingProcessingException(
                    BookingConstants.ProcessingError.SLOT_NOT_RESERVED,
                    "Booking has no reserved slot"
            );
        }
        if (booking.isSlotReleased()) {
            throw new NonRecoverableBookingProcessingException(
                    BookingConstants.ProcessingError.SLOT_ALREADY_RELEASED,
                    "Booking slot was already released"
            );
        }

        // Placeholder for the real downstream confirmation call. The method is intentionally
        // isolated so later integrations can classify provider errors without changing the
        // Kafka consumer/state-transition contract.
        return CompletableFuture.completedFuture(null);
    }
}
