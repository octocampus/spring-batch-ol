package io.openlineage.datastage.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectionDetail(ConnectionEntity entity) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConnectionEntity(ConnectionProperties properties) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConnectionProperties(
            String url,
            String host,
            Integer port,
            String database,
            @JsonProperty("connection_type") String connectionType) {}
}
