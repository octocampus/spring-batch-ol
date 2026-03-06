package io.openlineage.datastage.polling;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public class RunStateTracker {

    private final ConcurrentHashMap<String, TrackedRun> runs = new ConcurrentHashMap<>();

    public TrackedRun getOrCreate(String jobId, String jobName, String runId, String flowId) {
        String key = jobId + ":" + runId;
        return runs.computeIfAbsent(key, k -> new TrackedRun(jobId, jobName, runId, flowId));
    }

    public TrackedRun get(String jobId, String runId) {
        return runs.get(jobId + ":" + runId);
    }

    public void evictExpired(Duration retention) {
        runs.entrySet().removeIf(entry -> entry.getValue().isExpired(retention));
    }
}
