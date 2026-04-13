package com.example.batch.service;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.Booking;
import com.example.batch.domain.BookingStatus;
import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import com.example.batch.processing.BookingConfirmationCommand;
import com.example.batch.processing.BookingConfirmationGateway;
import com.example.batch.processing.BookingConfirmationResult;
import org.springframework.stereotype.Service;

@Service
public class RetryBookingProcessingService {

    private final BookingConfirmationGateway bookingConfirmationGateway;

    public RetryBookingProcessingService(BookingConfirmationGateway bookingConfirmationGateway) {
        this.bookingConfirmationGateway = bookingConfirmationGateway;
    }

    public RetryProcessingResult retry(RetryRecord retryRecord, Booking booking) {
        validateRetryCanRun(retryRecord, booking);

        BookingConfirmationResult confirmationResult = bookingConfirmationGateway.confirm(new BookingConfirmationCommand(
                booking.getBookingId(),
                booking.getDriverId(),
                booking.getOpportunityId(),
                retryRecord.getId(),
                retryRecord.getRetryCount()
        ));
        validateConfirmationResult(retryRecord, booking, confirmationResult);

        return new RetryProcessingResult(
                confirmationResult.bookingId(),
                confirmationResult.retryRecordId(),
                confirmationResult.confirmedAt()
        );
    }

    private void validateRetryCanRun(RetryRecord retryRecord, Booking booking) {
        if (retryRecord.getRetryStatus() != RetryStatus.RETRY_PENDING) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_RECORD_NOT_PENDING);
        }
        if (!retryRecord.getBookingId().equals(booking.getBookingId())
                || !retryRecord.getDriverId().equals(booking.getDriverId())
                || !retryRecord.getOpportunityId().equals(booking.getOpportunityId())) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_BOOKING_MISMATCH);
        }
        if (booking.getStatus() != BookingStatus.PENDING && booking.getStatus() != BookingStatus.PROCESSING) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_BOOKING_NOT_RETRYABLE);
        }
        if (!booking.isSlotReserved()) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_SLOT_NOT_RESERVED);
        }
        if (booking.isSlotReleased()) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_SLOT_ALREADY_RELEASED);
        }
    }

    private void validateConfirmationResult(
            RetryRecord retryRecord,
            Booking booking,
            BookingConfirmationResult confirmationResult
    ) {
        if (!booking.getBookingId().equals(confirmationResult.bookingId())
                || !retryRecord.getId().equals(confirmationResult.retryRecordId())
                || !booking.getDriverId().equals(confirmationResult.driverId())
                || !booking.getOpportunityId().equals(confirmationResult.opportunityId())) {
            throw new NonRecoverableRetryProcessingException(BatchConstants.Error.RETRY_BOOKING_MISMATCH);
        }
    }
}
