package io.openlineage.datastage.dsjob.polling;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public class RunStateTracker {

    private final ConcurrentHashMap<String, TrackedRun> runs = new ConcurrentHashMap<>();

    public TrackedRun getOrCreate(String project, String jobName, int waveNumber) {
        String key = project + ":" + jobName + ":" + waveNumber;
        return runs.computeIfAbsent(key, k -> new TrackedRun(project, jobName, waveNumber));
    }

    public void evictExpired(Duration retention) {
        runs.entrySet().removeIf(entry -> entry.getValue().isExpired(retention));
    }
}
