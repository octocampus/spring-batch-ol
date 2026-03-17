package io.openlineage.datastage.dsodb.xmeta;

public record XmetaStageRow(
        String jobName,
        String stageName,
        String stageType,
        int inputLinks,
        int outputLinks,
        String tableName,
        String server,
        String port,
        String database,
        String connectionType) {
}
