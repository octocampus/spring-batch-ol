package io.openlineage.batch.event;

import io.openlineage.batch.config.OpenLineageProperties;
import io.openlineage.batch.extractor.DatasetInfo;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class OpenLineageEventEmitter {

    private static final Logger log = LoggerFactory.getLogger(OpenLineageEventEmitter.class);

    private final OpenLineageClient client;
    private final OpenLineage ol;
    private final OpenLineageProperties properties;
    private final RunContext runContext;

    public OpenLineageEventEmitter(
            OpenLineageClient client,
            OpenLineage ol,
            OpenLineageProperties properties,
            RunContext runContext) {
        this.client = client;
        this.ol = ol;
        this.properties = properties;
        this.runContext = runContext;
    }

    public void emitJobStart(JobExecution jobExecution) {
        try {
            UUID runId = runContext.getOrCreateJobRunId(jobExecution.getId());
            String jobName = jobExecution.getJobInstance().getJobName();

            OpenLineage.RunFacets runFacets = ol.newRunFacetsBuilder()
                    .nominalTime(ol.newNominalTimeRunFacet(
                            ZonedDateTime.now(), null))
                    .build();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.START)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(runId, runFacets))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(jobName)
                            .build())
                    .inputs(Collections.emptyList())
                    .outputs(Collections.emptyList())
                    .build();

            client.emit(event);
            log.debug("Emitted Job START event for {} (runId={})", jobName, runId);
        } catch (Exception e) {
            log.warn("Failed to emit Job START event", e);
        }
    }

    public void emitJobComplete(JobExecution jobExecution) {
        try {
            UUID runId = runContext.getOrCreateJobRunId(jobExecution.getId());
            String jobName = jobExecution.getJobInstance().getJobName();
            RunEvent.EventType eventType = jobExecution.getStatus() == BatchStatus.COMPLETED
                    ? RunEvent.EventType.COMPLETE
                    : RunEvent.EventType.FAIL;

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(eventType)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(runId, null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(jobName)
                            .build())
                    .inputs(Collections.emptyList())
                    .outputs(Collections.emptyList())
                    .build();

            client.emit(event);
            log.debug("Emitted Job {} event for {} (runId={})", eventType, jobName, runId);
            runContext.cleanupJob(jobExecution.getId());
        } catch (Exception e) {
            log.warn("Failed to emit Job COMPLETE/FAIL event", e);
        }
    }

    public void emitStepStart(StepExecution stepExecution, List<DatasetInfo> inputs, List<DatasetInfo> outputs) {
        try {
            UUID stepRunId = runContext.getOrCreateStepRunId(stepExecution.getId());
            UUID jobRunId = runContext.getJobRunId(stepExecution.getJobExecution().getId());
            String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
            String stepJobName = jobName + "." + stepExecution.getStepName();

            OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder()
                    .nominalTime(ol.newNominalTimeRunFacet(ZonedDateTime.now(), null));

            if (jobRunId != null) {
                runFacetsBuilder.parent(ol.newParentRunFacet(
                        ol.newParentRunFacetRun(jobRunId),
                        ol.newParentRunFacetJob(properties.getNamespace(), jobName)));
            }

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.START)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(stepRunId, runFacetsBuilder.build()))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(stepJobName)
                            .build())
                    .inputs(toInputDatasets(inputs))
                    .outputs(toOutputDatasets(outputs))
                    .build();

            client.emit(event);
            log.debug("Emitted Step START event for {} (runId={})", stepJobName, stepRunId);
        } catch (Exception e) {
            log.warn("Failed to emit Step START event", e);
        }
    }

    public void emitStepComplete(StepExecution stepExecution, List<DatasetInfo> inputs, List<DatasetInfo> outputs) {
        try {
            UUID stepRunId = runContext.getOrCreateStepRunId(stepExecution.getId());
            UUID jobRunId = runContext.getJobRunId(stepExecution.getJobExecution().getId());
            String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
            String stepJobName = jobName + "." + stepExecution.getStepName();

            RunEvent.EventType eventType = stepExecution.getStatus() == BatchStatus.COMPLETED
                    ? RunEvent.EventType.COMPLETE
                    : RunEvent.EventType.FAIL;

            OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
            if (jobRunId != null) {
                runFacetsBuilder.parent(ol.newParentRunFacet(
                        ol.newParentRunFacetRun(jobRunId),
                        ol.newParentRunFacetJob(properties.getNamespace(), jobName)));
            }

            List<OutputDataset> outputDatasets = toOutputDatasets(outputs).stream()
                    .map(ds -> enrichWithStepMetrics(ds, stepExecution))
                    .toList();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(eventType)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(stepRunId, runFacetsBuilder.build()))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(stepJobName)
                            .build())
                    .inputs(toInputDatasets(inputs))
                    .outputs(outputDatasets)
                    .build();

            client.emit(event);
            log.debug("Emitted Step {} event for {} (runId={})", eventType, stepJobName, stepRunId);
            runContext.cleanupStep(stepExecution.getId());
        } catch (Exception e) {
            log.warn("Failed to emit Step COMPLETE/FAIL event", e);
        }
    }

    public void emitChunkRunning(StepExecution stepExecution, List<DatasetInfo> outputs) {
        try {
            UUID stepRunId = runContext.getOrCreateStepRunId(stepExecution.getId());
            String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
            String stepJobName = jobName + "." + stepExecution.getStepName();

            List<OutputDataset> outputDatasets = toOutputDatasets(outputs).stream()
                    .map(ds -> enrichWithChunkMetrics(ds, stepExecution))
                    .toList();

            RunEvent event = ol.newRunEventBuilder()
                    .eventType(RunEvent.EventType.RUNNING)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRun(stepRunId, null))
                    .job(ol.newJobBuilder()
                            .namespace(properties.getNamespace())
                            .name(stepJobName)
                            .build())
                    .inputs(Collections.emptyList())
                    .outputs(outputDatasets)
                    .build();

            client.emit(event);
            log.debug("Emitted Chunk RUNNING event for {} (writeCount={})", stepJobName, stepExecution.getWriteCount());
        } catch (Exception e) {
            log.warn("Failed to emit Chunk RUNNING event", e);
        }
    }

    private OutputDataset enrichWithChunkMetrics(OutputDataset dataset, StepExecution stepExecution) {
        return ol.newOutputDatasetBuilder()
                .namespace(dataset.getNamespace())
                .name(dataset.getName())
                .facets(ol.newDatasetFacetsBuilder().build())
                .outputFacets(ol.newOutputDatasetOutputFacetsBuilder()
                        .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(
                                (long) stepExecution.getWriteCount(), null, null))
                        .build())
                .build();
    }

    private OutputDataset enrichWithStepMetrics(OutputDataset dataset, StepExecution stepExecution) {
        return ol.newOutputDatasetBuilder()
                .namespace(dataset.getNamespace())
                .name(dataset.getName())
                .facets(ol.newDatasetFacetsBuilder().build())
                .outputFacets(ol.newOutputDatasetOutputFacetsBuilder()
                        .outputStatistics(ol.newOutputStatisticsOutputDatasetFacet(
                                (long) stepExecution.getWriteCount(), null, null))
                        .build())
                .build();
    }

    private List<InputDataset> toInputDatasets(List<DatasetInfo> infos) {
        if (infos == null) return Collections.emptyList();
        return infos.stream()
                .filter(i -> i.type() == DatasetInfo.Type.INPUT)
                .map(i -> ol.newInputDatasetBuilder()
                        .namespace(i.namespace())
                        .name(i.name())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .build())
                .toList();
    }

    private List<OutputDataset> toOutputDatasets(List<DatasetInfo> infos) {
        if (infos == null) return Collections.emptyList();
        return infos.stream()
                .filter(i -> i.type() == DatasetInfo.Type.OUTPUT)
                .map(i -> ol.newOutputDatasetBuilder()
                        .namespace(i.namespace())
                        .name(i.name())
                        .facets(ol.newDatasetFacetsBuilder().build())
                        .build())
                .toList();
    }
}
