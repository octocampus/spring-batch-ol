package io.openlineage.datastage.polling;

import io.openlineage.datastage.flow.DatasetDescriptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class TrackedRun {

    public enum EmittedState {
        NONE, STARTED, RUNNING, TERMINAL
    }

    private final String jobId;
    private final String jobName;
    private final String runId;
    private final String flowId;
    private final UUID openLineageRunId;
    private volatile EmittedState emittedState;
    private volatile List<DatasetDescriptor> datasets;
    private volatile Instant completedAt;

    public TrackedRun(String jobId, String jobName, String runId, String flowId) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.runId = runId;
        this.flowId = flowId;
        this.openLineageRunId = UUID.randomUUID();
        this.emittedState = EmittedState.NONE;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getRunId() {
        return runId;
    }

    public String getFlowId() {
        return flowId;
    }

    public UUID getOpenLineageRunId() {
        return openLineageRunId;
    }

    public EmittedState getEmittedState() {
        return emittedState;
    }

    public void setEmittedState(EmittedState emittedState) {
        this.emittedState = emittedState;
    }

    public List<DatasetDescriptor> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<DatasetDescriptor> datasets) {
        this.datasets = datasets;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void markCompleted() {
        this.completedAt = Instant.now();
    }

    public boolean isExpired(Duration retention) {
        return completedAt != null && Instant.now().isAfter(completedAt.plus(retention));
    }

    public String key() {
        return jobId + ":" + runId;
    }
}
