package io.openlineage.datastage.dsjob.event;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.datastage.dsjob.command.JobReport;
import io.openlineage.datastage.dsjob.config.OpenLineageEmitterProperties;
import io.openlineage.datastage.dsjob.dsx.DatasetDescriptor;
import io.openlineage.datastage.dsjob.polling.TrackedRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataStageEventEmitter {

    private static final Logger log = LoggerFactory.getLogger(DataStageEventEmitter.class);

    private final OpenLineageClient client;
    private final OpenLineage ol;
    private final OpenLineageEmitterProperties properties;

    public DataStageEventEmitter(OpenLineageClient client, OpenLineage ol,
                                 OpenLineageEmitterProperties properties) {
        this.client = client;
        this.ol = ol;
        this.properties = properties;
    }

    public void emitStart(TrackedRun run) {
        try {
            String jobFqn = run.getProject() + "." + run.getJobName();

            OpenLineage.RunFacets runFacets = ol.newRunFacetsBuilder()
                    .nominalTime(ol.newNominalTimeRunFacet(ZonedDateTime.now(), null))
                    .build();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.START)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), runFacets))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(jobFqn)
                            .build())
                    .inputs(toInputDatasets(run))
                    .outputs(toOutputDatasets(run, null))
                    .build();

            client.emit(event);
            log.debug("Emitted START for job={} wave={}", jobFqn, run.getWaveNumber());
        } catch (Exception e) {
            log.warn("Failed to emit START event for job={}", run.getJobName(), e);
        }
    }

    public void emitRunning(TrackedRun run, JobReport report) {
        try {
            String jobFqn = run.getProject() + "." + run.getJobName();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.RUNNING)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(jobFqn)
                            .build())
                    .inputs(toInputDatasets(run))
                    .outputs(toOutputDatasets(run, report))
                    .build();

            client.emit(event);
            log.debug("Emitted RUNNING for job={} wave={} rowsWritten={}",
                    jobFqn, run.getWaveNumber(), report != null ? report.totalRowsWritten() : 0);
        } catch (Exception e) {
            log.warn("Failed to emit RUNNING event for job={}", run.getJobName(), e);
        }
    }

    public void emitComplete(TrackedRun run, JobReport report, boolean success) {
        try {
            String jobFqn = run.getProject() + "." + run.getJobName();
            RunEvent.EventType eventType = success
                    ? RunEvent.EventType.COMPLETE
                    : RunEvent.EventType.FAIL;

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(eventType)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(jobFqn)
                            .build())
                    .inputs(toInputDatasets(run))
                    .outputs(toOutputDatasets(run, report))
                    .build();

            client.emit(event);
            log.debug("Emitted {} for job={} wave={} rowsWritten={}",
                    eventType, jobFqn, run.getWaveNumber(),
                    report != null ? report.totalRowsWritten() : 0);
        } catch (Exception e) {
            log.warn("Failed to emit COMPLETE/FAIL event for job={}", run.getJobName(), e);
        }
    }

    private List<InputDataset> toInputDatasets(TrackedRun run) {
        List<DatasetDescriptor> datasets = run.getDatasets();
        if (datasets == null || datasets.isEmpty()) {
            return Collections.emptyList();
        }

        return datasets.stream()
                .filter(d -> d.type() == DatasetDescriptor.Type.INPUT)
                .map(d -> ol.newInputDatasetBuilder()
                        .namespace(d.namespace())
                        .name(d.name())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .build())
                .toList();
    }

    private List<OutputDataset> toOutputDatasets(TrackedRun run, JobReport report) {
        List<DatasetDescriptor> datasets = run.getDatasets();

        // If we have DSX datasets, use them with row count enrichment from the report
        if (datasets != null && !datasets.isEmpty()) {
            Map<String, Long> rowsByStage = buildRowsByStage(report);

            return datasets.stream()
                    .filter(d -> d.type() == DatasetDescriptor.Type.OUTPUT)
                    .map(d -> {
                        Long rows = rowsByStage.getOrDefault(d.stageName(), 0L);
                        return ol.newOutputDatasetBuilder()
                                .namespace(d.namespace())
                                .name(d.name())
                                .facets(ol.newDatasetFacetsBuilder().build())
                                .outputFacets(ol.newOutputDatasetOutputFacetsBuilder()
                                        .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(
                                                rows, null, null))
                                        .build())
                                .build();
                    })
                    .toList();
        }

        // Fallback: use stage names from report as dataset names
        return buildFallbackOutputs(run, report);
    }

    private List<OutputDataset> buildFallbackOutputs(TrackedRun run, JobReport report) {
        if (report == null || report.stages().isEmpty()) {
            return Collections.emptyList();
        }

        String jobFqn = run.getProject() + "." + run.getJobName();
        return report.stages().stream()
                .filter(s -> s.rowsWritten() > 0)
                .map(stage -> ol.newOutputDatasetBuilder()
                        .namespace(properties.getNamespace())
                        .name(jobFqn + "." + stage.stageName())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .outputFacets(ol.newOutputDatasetOutputFacetsBuilder()
                                .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(
                                        stage.rowsWritten(), null, null))
                                .build())
                        .build())
                .toList();
    }

    private Map<String, Long> buildRowsByStage(JobReport report) {
        if (report == null || report.stages().isEmpty()) {
            return Map.of();
        }
        return report.stages().stream()
                .collect(Collectors.toMap(
                        JobReport.StageReport::stageName,
                        JobReport.StageReport::rowsWritten,
                        Long::sum));
    }
}
