package io.openlineage.datastage.dsjob.polling;

import io.openlineage.datastage.dsjob.command.DsjobRunner;
import io.openlineage.datastage.dsjob.command.JobInfo;
import io.openlineage.datastage.dsjob.command.JobReport;
import io.openlineage.datastage.dsjob.config.DsjobProperties;
import io.openlineage.datastage.dsjob.dsx.DatasetDescriptor;
import io.openlineage.datastage.dsjob.dsx.DsxParser;
import io.openlineage.datastage.dsjob.event.DataStageEventEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DsjobPoller {

    private static final Logger log = LoggerFactory.getLogger(DsjobPoller.class);

    private final DsjobRunner runner;
    private final RunStateTracker tracker;
    private final DataStageEventEmitter emitter;
    private final DsjobProperties properties;
    private final DsxParser dsxParser;

    private volatile Map<String, List<DatasetDescriptor>> dsxDatasets;

    public DsjobPoller(DsjobRunner runner, RunStateTracker tracker,
                       DataStageEventEmitter emitter, DsjobProperties properties,
                       DsxParser dsxParser) {
        this.runner = runner;
        this.tracker = tracker;
        this.emitter = emitter;
        this.properties = properties;
        this.dsxParser = dsxParser;
        this.dsxDatasets = loadDsxDatasets();
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

    /**
     * Reloads DSX files. Can be called to pick up new/changed exports.
     */
    public void reloadDsx() {
        this.dsxDatasets = loadDsxDatasets();
    }

    private Map<String, List<DatasetDescriptor>> loadDsxDatasets() {
        String dsxDir = properties.getDsxDirectory();
        if (dsxDir == null || dsxDir.isBlank()) {
            log.info("No DSX directory configured — dataset names will be stage-based");
            return Map.of();
        }
        return dsxParser.parseDirectory(Path.of(dsxDir));
    }

    private void doPoll() {
        List<String> projects = properties.getProjects();
        if (projects == null || projects.isEmpty()) {
            log.warn("No DataStage projects configured — skipping poll");
            return;
        }

        for (String project : projects) {
            pollProject(project);
        }
    }

    private void pollProject(String project) {
        List<String> jobs = runner.listJobs(project);
        if (jobs.isEmpty()) {
            log.debug("No jobs found in project {}", project);
            return;
        }

        log.debug("Found {} jobs in project {}", jobs.size(), project);

        for (String jobName : jobs) {
            try {
                processJob(project, jobName);
            } catch (Exception e) {
                log.warn("Failed to process job {}/{}", project, jobName, e);
            }
        }
    }

    private void processJob(String project, String jobName) {
        JobInfo info = runner.getJobInfo(project, jobName);
        if (info == null || !info.isActionable()) {
            return;
        }

        TrackedRun tracked = tracker.getOrCreate(project, jobName, info.waveNumber());

        if (tracked.getEmittedState() == TrackedRun.EmittedState.TERMINAL) {
            return;
        }

        // Attach DSX datasets if not yet done
        if (tracked.getDatasets() == null) {
            List<DatasetDescriptor> datasets = dsxDatasets.get(jobName);
            tracked.setDatasets(datasets);
        }

        if (info.isQueued()) {
            return;
        }

        if (tracked.getEmittedState() == TrackedRun.EmittedState.NONE) {
            if (info.isRunning()) {
                emitter.emitStart(tracked);
                tracked.setEmittedState(TrackedRun.EmittedState.STARTED);
            } else if (info.isCompleted()) {
                emitter.emitStart(tracked);
                JobReport report = runner.getJobReport(project, jobName);
                emitter.emitComplete(tracked, report, true);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            } else if (info.isFailed()) {
                emitter.emitStart(tracked);
                JobReport report = runner.getJobReport(project, jobName);
                emitter.emitComplete(tracked, report, false);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        } else if (tracked.getEmittedState() == TrackedRun.EmittedState.STARTED
                || tracked.getEmittedState() == TrackedRun.EmittedState.RUNNING) {
            if (info.isRunning()) {
                JobReport report = runner.getJobReport(project, jobName);
                emitter.emitRunning(tracked, report);
                tracked.setEmittedState(TrackedRun.EmittedState.RUNNING);
            } else if (info.isCompleted()) {
                JobReport report = runner.getJobReport(project, jobName);
                emitter.emitComplete(tracked, report, true);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            } else if (info.isFailed()) {
                JobReport report = runner.getJobReport(project, jobName);
                emitter.emitComplete(tracked, report, false);
                tracked.setEmittedState(TrackedRun.EmittedState.TERMINAL);
                tracked.markCompleted();
            }
        }
    }
}
