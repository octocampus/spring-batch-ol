package io.openlineage.batch.listener;

import io.openlineage.batch.config.OpenLineageProperties;
import io.openlineage.batch.event.OpenLineageEventEmitter;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class OpenLineageJobListener implements JobExecutionListener {

    private final OpenLineageEventEmitter emitter;
    private final OpenLineageProperties properties;

    public OpenLineageJobListener(OpenLineageEventEmitter emitter, OpenLineageProperties properties) {
        this.emitter = emitter;
        this.properties = properties;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        if (properties.getGranularity().isJobEvents()) {
            emitter.emitJobStart(jobExecution);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (properties.getGranularity().isJobEvents()) {
            emitter.emitJobComplete(jobExecution);
        }
    }
}
