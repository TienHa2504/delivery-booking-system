package com.example.batch.job.retry;

import com.example.batch.common.BatchConstants;
import com.example.batch.domain.RetryRecord;
import com.example.batch.domain.RetryStatus;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Instant;
import java.util.Map;

@Configuration
public class RetryJobConfig {

    @Bean
    Job retryJob(JobRepository jobRepository, @Qualifier("retryStep") Step retryStep) {
        return new JobBuilder(BatchConstants.Job.RETRY_JOB, jobRepository)
                .start(retryStep)
                .build();
    }

    @Bean
    Step retryStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            JpaPagingItemReader<RetryRecord> retryRecordReader,
            RetryRecordProcessor retryRecordProcessor,
            RetryRecordWriter retryRecordWriter
    ) {
        return new StepBuilder(BatchConstants.Job.RETRY_STEP, jobRepository)
                .<RetryRecord, RetryJobItem>chunk(50, transactionManager)
                .reader(retryRecordReader)
                .processor(retryRecordProcessor)
                .writer(retryRecordWriter)
                .build();
    }

    @Bean
    @StepScope
    JpaPagingItemReader<RetryRecord> retryRecordReader(
            EntityManagerFactory entityManagerFactory,
            @Value(BatchConstants.Retry.PAGE_SIZE) int pageSize
    ) {
        return new JpaPagingItemReaderBuilder<RetryRecord>()
                .name("retryRecordReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("""
                        select r
                        from RetryRecord r
                        where r.retryStatus = :retryStatus
                          and r.nextRetryAt <= :now
                        order by r.nextRetryAt asc, r.createdAt asc
                        """)
                .parameterValues(Map.of(
                        "retryStatus", RetryStatus.RETRY_PENDING,
                        "now", Instant.now()
                ))
                .pageSize(pageSize)
                .build();
    }
}
