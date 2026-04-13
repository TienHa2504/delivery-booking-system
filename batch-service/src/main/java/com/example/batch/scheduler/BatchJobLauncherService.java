package com.example.batch.scheduler;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class BatchJobLauncherService {

    private final JobLauncher jobLauncher;
    private final Job retryJob;
    private final Job statusReconciliationJob;
    private final Job remainingReconciliationJob;

    public BatchJobLauncherService(
            JobLauncher jobLauncher,
            @Qualifier("retryJob") Job retryJob,
            @Qualifier("statusReconciliationJob") Job statusReconciliationJob,
            @Qualifier("remainingReconciliationJob") Job remainingReconciliationJob
    ) {
        this.jobLauncher = jobLauncher;
        this.retryJob = retryJob;
        this.statusReconciliationJob = statusReconciliationJob;
        this.remainingReconciliationJob = remainingReconciliationJob;
    }

    public void runRetryJob() {
        run(retryJob);
    }

    public void runStatusReconciliationJob() {
        run(statusReconciliationJob);
    }

    public void runRemainingReconciliationJob() {
        run(remainingReconciliationJob);
    }

    private void run(Job job) {
        try {
            JobParameters parameters = new JobParametersBuilder()
                    .addString("triggeredAt", Instant.now().toString())
                    .toJobParameters();
            JobExecution jobExecution = jobLauncher.run(job, parameters);
            if (jobExecution.getStatus().isUnsuccessful()) {
                throw new IllegalStateException("Batch job ended with status " + jobExecution.getStatus());
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Batch job failed: " + job.getName(), ex);
        }
    }
}
