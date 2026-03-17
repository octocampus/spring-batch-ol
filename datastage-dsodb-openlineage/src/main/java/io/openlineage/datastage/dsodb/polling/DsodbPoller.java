package io.openlineage.datastage.dsodb.polling;

import io.openlineage.datastage.dsodb.config.DsodbProperties;
import io.openlineage.datastage.dsodb.dataset.DatasetResolverChain;
import io.openlineage.datastage.dsodb.dsx.DatasetDescriptor;
import io.openlineage.datastage.dsodb.event.DataStageEventEmitter;
import io.openlineage.datastage.dsodb.repository.DsodbJobRepository;
import io.openlineage.datastage.dsodb.repository.JobExecRow;
import io.openlineage.datastage.dsodb.repository.LinkResultRow;
import io.openlineage.datastage.dsodb.repository.StageResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class DsodbPoller {

    private static final Logger log = LoggerFactory.getLogger(DsodbPoller.class);

    private final DsodbJobRepository repository;
    private final RunStateTracker tracker;
    private final DataStageEventEmitter emitter;
    private final DsodbProperties properties;
    private final DatasetResolverChain datasetResolver;

    private volatile Timestamp lastPollTimestamp;

    public DsodbPoller(DsodbJobRepository repository, RunStateTracker tracker,
                       DataStageEventEmitter emitter, DsodbProperties properties,
                       DatasetResolverChain datasetResolver) {
        this.repository = repository;
        this.tracker = tracker;
        this.emitter = emitter;
        this.properties = properties;
        this.datasetResolver = datasetResolver;

        Instant lookbackStart = Instant.now().minus(properties.getPolling().getLookback());
        this.lastPollTimestamp = Timestamp.from(lookbackStart);
    }

    @Scheduled(fixedDelayString = "${datastage.polling.interval:PT30S}")
    public void poll() {
        try {
            doPoll();
        } catch (Exception e) {
            log.warn("Poll cycle failed", e);
        } finally {
            tracker.evictExpired(properties.getPolling().getRunRetention());
        }
    }

    private void doPoll() {
        List<String> projects = properties.getProjects();
        if (projects == null || projects.isEmpty()) {
            log.warn("No DataStage projects configured — skipping poll");
            return;
        }

        String dsodbUrl = properties.getDsodb().getUrl();
        if (dsodbUrl == null || dsodbUrl.isBlank()) {
            log.warn("DSODB JDBC URL not configured — skipping poll");
            return;
        }

        Timestamp pollStartTime = Timestamp.from(Instant.now());

        List<JobExecRow> runs = repository.findChangedRuns(projects, lastPollTimestamp);
        log.debug("Found {} changed runs since {}", runs.size(), lastPollTimestamp);

        for (JobExecRow exec : runs) {
            try {
                processRun(exec);
            } catch (Exception e) {
                log.warn("Failed to process run {}/{} wave={}",
                        exec.projectName(), exec.jobName(), exec.waveNo(), e);
            }
        }

        this.lastPollTimestamp = pollStartTime;
    }

    private void processRun(JobExecRow exec) {
        TrackedRun tracked = tracker.getOrCreate(exec.projectName(), exec.jobName(), exec.waveNo());

        if (tracked.getEmittedState() == TrackedRun.EmittedState.TERMINAL) {
            return;
        }

        if (exec.runStartTime() != null) {
            tracked.setRunStartTime(exec.runStartTime().toInstant());
        }
        if (exec.runEndTime() != null) {
            tracked.setRunEndTime(exec.runEndTime().toInstant());
        }

        if (tracked.getDatasets() == null) {
            List<DatasetDescriptor> datasets = datasetResolver.resolve(
                    exec.projectName(), exec.jobName());
            tracked.setDatasets(datasets);
        }

        if (tracked.getEmittedState() == TrackedRun.EmittedState.NONE) {
            if (exec.isRunning()) {
                emitter.emitStart(tracked);
                tracked.setEmittedState(TrackedRun.EmittedState.STARTED);
            } else if (exec.isTerminal()) {
                emitter.emitStart(tracked);
                List<StageResultRow> stages = repository.findStageResults(exec.jobName(), exec.waveNo());
                emitter.emitComplete(tracked, stages, exec.isSuccess());
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        } else if (tracked.getEmittedState() == TrackedRun.EmittedState.STARTED
                || tracked.getEmittedState() == TrackedRun.EmittedState.RUNNING) {
            if (exec.isRunning()) {
                List<StageResultRow> stages = repository.findStageResults(exec.jobName(), exec.waveNo());
                emitter.emitRunning(tracked, stages);
                tracked.setEmittedState(TrackedRun.EmittedState.RUNNING);
            } else if (exec.isTerminal()) {
                List<StageResultRow> stages = repository.findStageResults(exec.jobName(), exec.waveNo());
                emitter.emitComplete(tracked, stages, exec.isSuccess());
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        }
    }
}
