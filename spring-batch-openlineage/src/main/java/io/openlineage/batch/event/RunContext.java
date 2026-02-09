package io.openlineage.batch.event;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RunContext {

    private final ConcurrentHashMap<Long, UUID> jobRunIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, UUID> stepRunIds = new ConcurrentHashMap<>();

    public UUID getOrCreateJobRunId(Long jobExecutionId) {
        return jobRunIds.computeIfAbsent(jobExecutionId, id -> UUID.randomUUID());
    }

    public UUID getOrCreateStepRunId(Long stepExecutionId) {
        return stepRunIds.computeIfAbsent(stepExecutionId, id -> UUID.randomUUID());
    }

    public UUID getJobRunId(Long jobExecutionId) {
        return jobRunIds.get(jobExecutionId);
    }

    public void cleanupJob(Long jobExecutionId) {
        jobRunIds.remove(jobExecutionId);
    }

    public void cleanupStep(Long stepExecutionId) {
        stepRunIds.remove(stepExecutionId);
    }
}
