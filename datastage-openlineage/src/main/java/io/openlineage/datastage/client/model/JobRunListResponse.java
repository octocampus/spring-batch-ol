package io.openlineage.datastage.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JobRunListResponse(List<RunResult> results) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RunResult(RunMetadata metadata, RunEntity entity) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RunMetadata(@JsonProperty("asset_id") String assetId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RunEntity(@JsonProperty("job_run") JobRun jobRun) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JobRun(String state) {}
}
