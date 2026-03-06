package io.openlineage.datastage.event;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.datastage.client.model.JobRunDetail;
import io.openlineage.datastage.config.OpenLineageEmitterProperties;
import io.openlineage.datastage.flow.DatasetDescriptor;
import io.openlineage.datastage.polling.TrackedRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

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
            OpenLineage.RunFacets runFacets = ol.newRunFacetsBuilder()
                    .nominalTime(ol.newNominalTimeRunFacet(ZonedDateTime.now(), null))
                    .build();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.START)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), runFacets))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(run.getJobName())
                            .build())
                    .inputs(toInputDatasets(run.getDatasets()))
                    .outputs(toOutputDatasets(run.getDatasets()))
                    .build();

            client.emit(event);
            log.debug("Emitted START for job={} run={}", run.getJobName(), run.getRunId());
        } catch (Exception e) {
            log.warn("Failed to emit START event for job={} run={}", run.getJobName(), run.getRunId(), e);
        }
    }

    public void emitRunning(TrackedRun run, JobRunDetail detail) {
        try {
            List<OutputDataset> outputs = toOutputDatasets(run.getDatasets()).stream()
                    .map(ds -> enrichWithMetrics(ds, detail))
                    .toList();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.RUNNING)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(run.getJobName())
                            .build())
                    .inputs(toInputDatasets(run.getDatasets()))
                    .outputs(outputs)
                    .build();

            client.emit(event);
            log.debug("Emitted RUNNING for job={} run={}", run.getJobName(), run.getRunId());
        } catch (Exception e) {
            log.warn("Failed to emit RUNNING event for job={} run={}", run.getJobName(), run.getRunId(), e);
        }
    }

    public void emitComplete(TrackedRun run, JobRunDetail detail, boolean success) {
        try {
            RunEvent.EventType eventType = success
                    ? RunEvent.EventType.COMPLETE
                    : RunEvent.EventType.FAIL;

            List<OutputDataset> outputs = toOutputDatasets(run.getDatasets()).stream()
                    .map(ds -> enrichWithMetrics(ds, detail))
                    .toList();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(eventType)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(run.getOpenLineageRunId(), null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(run.getJobName())
                            .build())
                    .inputs(toInputDatasets(run.getDatasets()))
                    .outputs(outputs)
                    .build();

            client.emit(event);
            log.debug("Emitted {} for job={} run={}", eventType, run.getJobName(), run.getRunId());
        } catch (Exception e) {
            log.warn("Failed to emit COMPLETE/FAIL event for job={} run={}", run.getJobName(), run.getRunId(), e);
        }
    }

    private OutputDataset enrichWithMetrics(OutputDataset dataset, JobRunDetail detail) {
        long totalRowsWritten = 0;
        if (detail != null && detail.entity() != null && detail.entity().jobRun() != null
                && detail.entity().jobRun().configuration() != null
                && detail.entity().jobRun().configuration().metrics() != null
                && detail.entity().jobRun().configuration().metrics().stageMetrics() != null) {
            totalRowsWritten = detail.entity().jobRun().configuration().metrics().stageMetrics().stream()
                    .filter(m -> m.rowsWritten() != null)
                    .mapToLong(JobRunDetail.StageMetric::rowsWritten)
                    .sum();
        }

        return ol.newOutputDatasetBuilder()
                .namespace(dataset.getNamespace())
                .name(dataset.getName())
                .facets(ol.newDatasetFacetsBuilder().build())
                .outputFacets(ol.newOutputDatasetOutputFacetsBuilder()
                        .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(
                                totalRowsWritten, null, null))
                        .build())
                .build();
    }

    private List<InputDataset> toInputDatasets(List<DatasetDescriptor> descriptors) {
        if (descriptors == null) return Collections.emptyList();
        return descriptors.stream()
                .filter(d -> d.type() == DatasetDescriptor.Type.INPUT)
                .map(d -> ol.newInputDatasetBuilder()
                        .namespace(d.namespace())
                        .name(d.name())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .build())
                .toList();
    }

    private List<OutputDataset> toOutputDatasets(List<DatasetDescriptor> descriptors) {
        if (descriptors == null) return Collections.emptyList();
        return descriptors.stream()
                .filter(d -> d.type() == DatasetDescriptor.Type.OUTPUT)
                .map(d -> ol.newOutputDatasetBuilder()
                        .namespace(d.namespace())
                        .name(d.name())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .build())
                .toList();
    }
}
