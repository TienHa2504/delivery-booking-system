package com.example.batch.job.reconciliation;

import com.example.batch.common.BatchConstants;
import com.example.batch.service.StatusReconciliationService;
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
public class StatusReconciliationJobConfig {

    @Bean
    Job statusReconciliationJob(
            JobRepository jobRepository,
            @Qualifier("statusReconciliationStep") Step statusReconciliationStep
    ) {
        return new JobBuilder(BatchConstants.Job.STATUS_RECONCILIATION_JOB, jobRepository)
                .start(statusReconciliationStep)
                .build();
    }

    @Bean
    Step statusReconciliationStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            StatusReconciliationService statusReconciliationService
    ) {
        return new StepBuilder(BatchConstants.Job.STATUS_RECONCILIATION_STEP, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    statusReconciliationService.reconcile();
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
