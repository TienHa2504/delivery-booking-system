package com.example.booking.config;

import com.example.booking.common.BookingConstants;
import com.example.booking.dto.BookingCreatedEvent;
import com.example.booking.dto.BookingDlqEvent;
import com.example.booking.exception.NonRecoverableBookingProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> bookingKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler bookingConsumerErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(bookingConsumerErrorHandler);
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> bookingDlqKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000L, 3L)));
        return factory;
    }

    @Bean
    DefaultErrorHandler bookingConsumerErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value(BookingConstants.Kafka.BOOKING_DLQ_TOPIC) String bookingDlqTopic
    ) {
        var recoverer = (org.springframework.kafka.listener.ConsumerRecordRecoverer) (record, ex) -> {
            BookingDlqEvent dlqEvent = toDlqEvent(record, ex);
            publishDlqEvent(kafkaTemplate, bookingDlqTopic, dlqEvent);
        };
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
        errorHandler.addNotRetryableExceptions(NonRecoverableBookingProcessingException.class);
        errorHandler.setAckAfterHandle(true);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    private BookingDlqEvent toDlqEvent(ConsumerRecord<?, ?> record, Exception ex) {
        Object value = record.value();
        if (value instanceof BookingCreatedEvent event) {
            return new BookingDlqEvent(
                    BookingConstants.EventType.BOOKING_RETRY_REQUESTED,
                    event.bookingId(),
                    event.driverId(),
                    event.opportunityId(),
                    BookingConstants.ProcessingError.BOOKING_PROCESSING_UNEXPECTED,
                    ex.getMessage(),
                    Instant.now()
            );
        }

        throw new IllegalArgumentException("Unsupported recoverable booking payload for DLQ: " + value);
    }

    private void publishDlqEvent(KafkaTemplate<Object, Object> kafkaTemplate, String bookingDlqTopic, BookingDlqEvent dlqEvent) {
        try {
            kafkaTemplate.send(bookingDlqTopic, dlqEvent.bookingId().toString(), dlqEvent)
                    .get(3, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing booking DLQ event", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Could not publish booking DLQ event", ex);
        }
    }
}
