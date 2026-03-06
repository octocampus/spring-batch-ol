package io.openlineage.datastage.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JobListResponse(List<JobResult> results) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JobResult(Metadata metadata, Entity entity) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Metadata(@JsonProperty("asset_id") String assetId, String name) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Entity(Job job) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Job(@JsonProperty("asset_ref") String assetRef) {}
}
