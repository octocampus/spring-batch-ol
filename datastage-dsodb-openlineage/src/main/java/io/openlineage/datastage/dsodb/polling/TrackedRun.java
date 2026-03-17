package io.openlineage.datastage.dsodb.polling;

import io.openlineage.datastage.dsodb.dsx.DatasetDescriptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class TrackedRun {

    public enum EmittedState { NONE, STARTED, RUNNING, TERMINAL }

    private final String project;
    private final String jobName;
    private final int waveNumber;
    private final UUID openLineageRunId;
    private volatile EmittedState emittedState;
    private volatile List<DatasetDescriptor> datasets;
    private volatile Instant completedAt;
    private volatile Instant runStartTime;
    private volatile Instant runEndTime;

    public TrackedRun(String project, String jobName, int waveNumber) {
        this.project = project;
        this.jobName = jobName;
        this.waveNumber = waveNumber;
        this.openLineageRunId = UUID.randomUUID();
        this.emittedState = EmittedState.NONE;
    }

    public String getProject() { return project; }
    public String getJobName() { return jobName; }
    public int getWaveNumber() { return waveNumber; }
    public UUID getOpenLineageRunId() { return openLineageRunId; }
    public EmittedState getEmittedState() { return emittedState; }
    public void setEmittedState(EmittedState emittedState) { this.emittedState = emittedState; }
    public List<DatasetDescriptor> getDatasets() { return datasets; }
    public void setDatasets(List<DatasetDescriptor> datasets) { this.datasets = datasets; }
    public Instant getRunStartTime() { return runStartTime; }
    public void setRunStartTime(Instant runStartTime) { this.runStartTime = runStartTime; }
    public Instant getRunEndTime() { return runEndTime; }
    public void setRunEndTime(Instant runEndTime) { this.runEndTime = runEndTime; }

    public void markCompleted() { this.completedAt = Instant.now(); }

    public boolean isExpired(Duration retention) {
        return completedAt != null && Instant.now().isAfter(completedAt.plus(retention));
    }

    public String key() { return project + ":" + jobName + ":" + waveNumber; }
}
