package io.openlineage.datastage.dsodb.repository;

public record LinkResultRow(
        String jobName,
        String stageName,
        String linkName,
        int waveNo,
        long rowCount) {
}
