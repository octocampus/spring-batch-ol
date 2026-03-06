package io.openlineage.datastage.polling;

import io.openlineage.datastage.client.DataStageClient;
import io.openlineage.datastage.client.model.*;
import io.openlineage.datastage.config.DataStageProperties;
import io.openlineage.datastage.event.DataStageEventEmitter;
import io.openlineage.datastage.flow.DatasetDescriptor;
import io.openlineage.datastage.flow.FlowParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DataStagePoller {

    private static final Logger log = LoggerFactory.getLogger(DataStagePoller.class);

    private static final Set<String> QUEUED_STATES = Set.of("Queued", "Starting");
    private static final Set<String> RUNNING_STATES = Set.of("Running");
    private static final Set<String> COMPLETED_STATES = Set.of("Completed", "CompletedWithWarnings");
    private static final Set<String> FAILED_STATES = Set.of("Failed", "Canceled", "Cancelled");

    private final DataStageClient client;
    private final FlowParser flowParser;
    private final RunStateTracker tracker;
    private final DataStageEventEmitter emitter;
    private final DataStageProperties properties;

    private final ConcurrentHashMap<String, String> jobFlowCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<DatasetDescriptor>> flowDatasetCache = new ConcurrentHashMap<>();

    public DataStagePoller(DataStageClient client, FlowParser flowParser, RunStateTracker tracker,
                           DataStageEventEmitter emitter, DataStageProperties properties) {
        this.client = client;
        this.flowParser = flowParser;
        this.tracker = tracker;
        this.emitter = emitter;
        this.properties = properties;
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
        if (properties.getApiKey() == null || properties.getApiKey().isBlank()) {
            log.warn("DataStage API key not configured — skipping poll");
            return;
        }
        if (properties.getProjectId() == null || properties.getProjectId().isBlank()) {
            log.warn("DataStage project ID not configured — skipping poll");
            return;
        }

        JobListResponse jobs = client.listJobs();
        if (jobs == null || jobs.results() == null) {
            log.debug("No jobs found");
            return;
        }

        for (var jobResult : jobs.results()) {
            String jobId = jobResult.metadata().assetId();
            String jobName = jobResult.metadata().name() != null
                    ? jobResult.metadata().name() : jobId;
            String flowId = jobResult.entity() != null && jobResult.entity().job() != null
                    ? jobResult.entity().job().assetRef() : null;

            if (flowId != null) {
                jobFlowCache.putIfAbsent(jobId, flowId);
            } else {
                flowId = jobFlowCache.get(jobId);
            }

            processJobRuns(jobId, jobName, flowId);
        }
    }

    private void processJobRuns(String jobId, String jobName, String flowId) {
        try {
            JobRunListResponse runs = client.listRuns(jobId);
            if (runs == null || runs.results() == null) return;

            for (var runResult : runs.results()) {
                String runId = runResult.metadata().assetId();
                String state = runResult.entity().jobRun().state();
                processRun(jobId, jobName, runId, flowId, state);
            }
        } catch (Exception e) {
            log.warn("Failed to process runs for job {}", jobId, e);
        }
    }

    private void processRun(String jobId, String jobName, String runId, String flowId, String state) {
        TrackedRun tracked = tracker.getOrCreate(jobId, jobName, runId, flowId);

        if (tracked.getEmittedState() == TrackedRun.EmittedState.TERMINAL) {
            return;
        }

        ensureDatasets(tracked, flowId);

        if (QUEUED_STATES.contains(state)) {
            // Not yet running — nothing to emit
            return;
        }

        if (tracked.getEmittedState() == TrackedRun.EmittedState.NONE) {
            if (RUNNING_STATES.contains(state)) {
                // First seen as running → emit START
                emitter.emitStart(tracked);
                tracked.setEmittedState(TrackedRun.EmittedState.STARTED);
            } else if (COMPLETED_STATES.contains(state)) {
                // First seen as completed → emit START + COMPLETE
                emitter.emitStart(tracked);
                JobRunDetail detail = fetchDetail(jobId, runId);
                emitter.emitComplete(tracked, detail, true);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            } else if (FAILED_STATES.contains(state)) {
                // First seen as failed → emit START + FAIL
                emitter.emitStart(tracked);
                JobRunDetail detail = fetchDetail(jobId, runId);
                emitter.emitComplete(tracked, detail, false);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        } else if (tracked.getEmittedState() == TrackedRun.EmittedState.STARTED
                || tracked.getEmittedState() == TrackedRun.EmittedState.RUNNING) {
            if (RUNNING_STATES.contains(state)) {
                // Still running → emit RUNNING
                JobRunDetail detail = fetchDetail(jobId, runId);
                emitter.emitRunning(tracked, detail);
                tracked.setEmittedState(TrackedRun.EmittedState.RUNNING);
            } else if (COMPLETED_STATES.contains(state)) {
                // Transitioned to completed → emit COMPLETE
                JobRunDetail detail = fetchDetail(jobId, runId);
                emitter.emitComplete(tracked, detail, true);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            } else if (FAILED_STATES.contains(state)) {
                // Transitioned to failed → emit FAIL
                JobRunDetail detail = fetchDetail(jobId, runId);
                emitter.emitComplete(tracked, detail, false);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        }
    }

    private void ensureDatasets(TrackedRun tracked, String flowId) {
        if (tracked.getDatasets() != null || flowId == null) {
            return;
        }

        List<DatasetDescriptor> datasets = flowDatasetCache.computeIfAbsent(flowId, fid -> {
            try {
                FlowDefinition flow = client.getFlowDefinition(fid);
                return flowParser.parse(flow);
            } catch (Exception e) {
                log.warn("Failed to parse flow {}", fid, e);
                return Collections.emptyList();
            }
        });

        tracked.setDatasets(datasets);
    }

    private JobRunDetail fetchDetail(String jobId, String runId) {
        try {
            return client.getRunDetail(jobId, runId);
        } catch (Exception e) {
            log.warn("Failed to fetch run detail for job={} run={}", jobId, runId, e);
            return null;
        }
    }
}
