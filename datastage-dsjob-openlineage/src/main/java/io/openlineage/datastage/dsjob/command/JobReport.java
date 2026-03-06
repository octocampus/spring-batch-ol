package io.openlineage.datastage.dsjob.command;

import java.util.List;

public record JobReport(List<StageReport> stages) {

    public record StageReport(String stageName, long rowsRead, long rowsWritten) {}

    public long totalRowsWritten() {
        return stages.stream().mapToLong(StageReport::rowsWritten).sum();
    }

    public long totalRowsRead() {
        return stages.stream().mapToLong(StageReport::rowsRead).sum();
    }
}
