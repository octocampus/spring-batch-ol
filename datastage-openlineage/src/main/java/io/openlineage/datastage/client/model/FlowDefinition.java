package io.openlineage.datastage.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FlowDefinition(FlowEntity entity) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FlowEntity(@JsonProperty("data_intg_flow") DataIntgFlow dataIntgFlow) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DataIntgFlow(@JsonProperty("pipeline_flows") List<PipelineFlow> pipelineFlows) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PipelineFlow(List<Pipeline> pipelines) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Pipeline(List<Node> nodes) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Node(
            @JsonProperty("node_type_id") String nodeTypeId,
            @JsonProperty("app_data") AppData appData) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AppData(Properties properties) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Properties(
            @JsonProperty("table_name") String tableName,
            @JsonProperty("file_path") String filePath,
            @JsonProperty("connection_ref") String connectionRef,
            @JsonProperty("is_source") Boolean isSource) {}
}
