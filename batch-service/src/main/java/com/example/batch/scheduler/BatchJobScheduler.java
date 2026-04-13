package com.example.batch.scheduler;

import com.example.batch.common.BatchConstants;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BatchJobScheduler {

    private final BatchJobLauncherService batchJobLauncherService;

    public BatchJobScheduler(BatchJobLauncherService batchJobLauncherService) {
        this.batchJobLauncherService = batchJobLauncherService;
    }

    @Scheduled(
            fixedDelayString = BatchConstants.Retry.SCHEDULE_FIXED_DELAY,
            initialDelayString = BatchConstants.Retry.SCHEDULE_INITIAL_DELAY
    )
    @SchedulerLock(
            name = BatchConstants.Job.RETRY_JOB,
            lockAtMostFor = BatchConstants.Retry.SCHEDULE_LOCK_AT_MOST_FOR,
            lockAtLeastFor = BatchConstants.Retry.SCHEDULE_LOCK_AT_LEAST_FOR
    )
    public void runRetryJob() {
        batchJobLauncherService.runRetryJob();
    }

    @Scheduled(
            fixedDelayString = BatchConstants.Reconciliation.SCHEDULE_FIXED_DELAY,
            initialDelayString = BatchConstants.Reconciliation.SCHEDULE_INITIAL_DELAY
    )
    @SchedulerLock(
            name = BatchConstants.Job.STATUS_RECONCILIATION_JOB,
            lockAtMostFor = BatchConstants.Reconciliation.SCHEDULE_LOCK_AT_MOST_FOR,
            lockAtLeastFor = BatchConstants.Reconciliation.SCHEDULE_LOCK_AT_LEAST_FOR
    )
    public void runStatusReconciliationJob() {
        batchJobLauncherService.runStatusReconciliationJob();
    }

    @Scheduled(
            fixedDelayString = BatchConstants.RemainingReconciliation.SCHEDULE_FIXED_DELAY,
            initialDelayString = BatchConstants.RemainingReconciliation.SCHEDULE_INITIAL_DELAY
    )
    @SchedulerLock(
            name = BatchConstants.Job.REMAINING_RECONCILIATION_JOB,
            lockAtMostFor = BatchConstants.RemainingReconciliation.SCHEDULE_LOCK_AT_MOST_FOR,
            lockAtLeastFor = BatchConstants.RemainingReconciliation.SCHEDULE_LOCK_AT_LEAST_FOR
    )
    public void runRemainingReconciliationJob() {
        batchJobLauncherService.runRemainingReconciliationJob();
    }
}
