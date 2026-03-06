package io.openlineage.datastage.dsjob.command;

public record JobInfo(
        int statusCode,
        String statusText,
        String startTime,
        String endTime,
        int waveNumber) {

    public boolean isRunning() {
        return statusCode == 0;
    }

    public boolean isCompleted() {
        return statusCode == 1 || statusCode == 2;
    }

    public boolean isFailed() {
        return statusCode == 3 || statusCode == 9;
    }

    public boolean isQueued() {
        return statusCode == 13;
    }

    public boolean isTerminal() {
        return isCompleted() || isFailed();
    }

    public boolean isActionable() {
        return isRunning() || isCompleted() || isFailed() || isQueued();
    }
}
