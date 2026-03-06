package io.openlineage.datastage.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRunDetail(RunDetailEntity entity) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RunDetailEntity(@JsonProperty("job_run") JobRunInfo jobRun) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JobRunInfo(
            String state,
            @JsonProperty("start_timestamp") String startTimestamp,
            @JsonProperty("end_timestamp") String endTimestamp,
            Configuration configuration) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Configuration(Metrics metrics) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Metrics(@JsonProperty("stage_metrics") List<StageMetric> stageMetrics) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StageMetric(
            @JsonProperty("stage_name") String stageName,
            @JsonProperty("rows_read") Long rowsRead,
            @JsonProperty("rows_written") Long rowsWritten) {}
}
