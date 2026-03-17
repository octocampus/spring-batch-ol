package io.openlineage.datastage.dsodb.repository;

public record StageResultRow(
        String jobName,
        String stageName,
        int waveNo,
        long rowsRead,
        long rowsWritten,
        long rowsRejected) {
}
