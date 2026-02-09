package io.openlineage.batch.listener;

import io.openlineage.batch.config.OpenLineageProperties;
import io.openlineage.batch.event.OpenLineageEventEmitter;
import io.openlineage.batch.extractor.DatasetInfo;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OpenLineageChunkListener implements ChunkListener {

    private final OpenLineageEventEmitter emitter;
    private final OpenLineageProperties properties;
    private final Map<Long, AtomicInteger> chunkCounters = new ConcurrentHashMap<>();

    // stepExecutionId -> output datasets (set by BeanPostProcessor via StepListener)
    private final Map<String, List<DatasetInfo>> stepOutputDatasets = new ConcurrentHashMap<>();

    public OpenLineageChunkListener(OpenLineageEventEmitter emitter, OpenLineageProperties properties) {
        this.emitter = emitter;
        this.properties = properties;
    }

    public void registerStepOutputDatasets(String stepName, List<DatasetInfo> outputs) {
        stepOutputDatasets.put(stepName, outputs);
    }

    @Override
    public void beforeChunk(ChunkContext context) {
        // no-op
    }

    @Override
    public void afterChunk(ChunkContext context) {
        if (!properties.getGranularity().isChunkEvents()) return;

        StepExecution stepExecution = context.getStepContext().getStepExecution();
        int interval = properties.getGranularity().getChunkEventInterval();

        AtomicInteger counter = chunkCounters.computeIfAbsent(
                stepExecution.getId(), k -> new AtomicInteger(0));
        int count = counter.incrementAndGet();

        if (count % interval != 0) return;

        List<DatasetInfo> outputs = stepOutputDatasets.getOrDefault(
                stepExecution.getStepName(), List.of());

        emitter.emitChunkRunning(stepExecution, outputs);
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        // no-op — errors will be reported via Step FAIL event
    }
}
