package com.example.batch.job.remaining;

import com.example.batch.common.BatchConstants;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class RemainingReconciliationJobConfig {

    @Bean
    Job remainingReconciliationJob(
            JobRepository jobRepository,
            @Qualifier("remainingReconciliationStep") Step remainingReconciliationStep
    ) {
        return new JobBuilder(BatchConstants.Job.REMAINING_RECONCILIATION_JOB, jobRepository)
                .start(remainingReconciliationStep)
                .build();
    }

    @Bean
    Step remainingReconciliationStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            RemainingReconciliationService remainingReconciliationService
    ) {
        return new StepBuilder(BatchConstants.Job.REMAINING_RECONCILIATION_STEP, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    remainingReconciliationService.reconcile();
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
