package io.openlineage.batch.listener;

import io.openlineage.batch.config.OpenLineageProperties;
import io.openlineage.batch.event.OpenLineageEventEmitter;
import io.openlineage.batch.extractor.CompositeDatasetExtractor;
import io.openlineage.batch.extractor.DatasetInfo;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OpenLineageStepListener implements StepExecutionListener {

    private final OpenLineageEventEmitter emitter;
    private final CompositeDatasetExtractor extractor;
    private final OpenLineageProperties properties;

    // stepName -> (reader, writer)
    private final Map<String, StepComponents> stepComponents = new ConcurrentHashMap<>();

    public record StepComponents(Object reader, Object writer) {}

    public OpenLineageStepListener(
            OpenLineageEventEmitter emitter,
            CompositeDatasetExtractor extractor,
            OpenLineageProperties properties) {
        this.emitter = emitter;
        this.extractor = extractor;
        this.properties = properties;
    }

    public void registerStepComponents(String stepName, Object reader, Object writer) {
        stepComponents.put(stepName, new StepComponents(reader, writer));
    }

    public Map<String, StepComponents> getStepComponents() {
        return stepComponents;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        if (!properties.getGranularity().isStepEvents()) return;

        List<DatasetInfo> inputs = new ArrayList<>();
        List<DatasetInfo> outputs = new ArrayList<>();
        extractDatasets(stepExecution.getStepName(), inputs, outputs);

        emitter.emitStepStart(stepExecution, inputs, outputs);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (!properties.getGranularity().isStepEvents()) return stepExecution.getExitStatus();

        List<DatasetInfo> inputs = new ArrayList<>();
        List<DatasetInfo> outputs = new ArrayList<>();
        extractDatasets(stepExecution.getStepName(), inputs, outputs);

        emitter.emitStepComplete(stepExecution, inputs, outputs);
        return stepExecution.getExitStatus();
    }

    private void extractDatasets(String stepName, List<DatasetInfo> inputs, List<DatasetInfo> outputs) {
        StepComponents components = stepComponents.get(stepName);
        if (components == null) return;

        if (components.reader() != null) {
            DatasetInfo info = extractor.extract(components.reader());
            inputs.add(info);
        }
        if (components.writer() != null) {
            DatasetInfo info = extractor.extract(components.writer());
            outputs.add(info);
        }
    }
}
