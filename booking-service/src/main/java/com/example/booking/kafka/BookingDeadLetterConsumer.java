package com.example.booking.kafka;

import com.example.booking.common.BookingConstants;
import com.example.booking.dto.BookingDlqEvent;
import com.example.booking.service.BookingStateTransitionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class BookingDeadLetterConsumer {

    private final BookingStateTransitionService bookingStateTransitionService;

    public BookingDeadLetterConsumer(BookingStateTransitionService bookingStateTransitionService) {
        this.bookingStateTransitionService = bookingStateTransitionService;
    }

    @KafkaListener(
            topics = BookingConstants.Kafka.BOOKING_DLQ_TOPIC,
            groupId = BookingConstants.Kafka.DLQ_CONSUMER_GROUP_ID,
            containerFactory = BookingConstants.Kafka.DLQ_LISTENER_CONTAINER_FACTORY
    )
    public void onDeadLetter(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object event = record.value();
        if (event instanceof BookingDlqEvent bookingDlqEvent) {
            bookingStateTransitionService.recordRetryFromDlq(bookingDlqEvent);
            acknowledgment.acknowledge();
            return;
        }

        acknowledgment.acknowledge();
    }
}
