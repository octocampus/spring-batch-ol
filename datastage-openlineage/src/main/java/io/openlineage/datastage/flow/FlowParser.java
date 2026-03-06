package io.openlineage.datastage.flow;

import io.openlineage.datastage.client.DataStageClient;
import io.openlineage.datastage.client.model.ConnectionDetail;
import io.openlineage.datastage.client.model.FlowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FlowParser {

    private static final Logger log = LoggerFactory.getLogger(FlowParser.class);

    private final DataStageClient client;
    private final ConcurrentHashMap<String, String> connectionNamespaceCache = new ConcurrentHashMap<>();

    public FlowParser(DataStageClient client) {
        this.client = client;
    }

    public List<DatasetDescriptor> parse(FlowDefinition flow) {
        List<DatasetDescriptor> datasets = new ArrayList<>();

        if (flow == null || flow.entity() == null || flow.entity().dataIntgFlow() == null) {
            return datasets;
        }

        var pipelineFlows = flow.entity().dataIntgFlow().pipelineFlows();
        if (pipelineFlows == null) {
            return datasets;
        }

        for (var pipelineFlow : pipelineFlows) {
            if (pipelineFlow.pipelines() == null) continue;
            for (var pipeline : pipelineFlow.pipelines()) {
                if (pipeline.nodes() == null) continue;
                for (var node : pipeline.nodes()) {
                    DatasetDescriptor descriptor = parseNode(node);
                    if (descriptor != null) {
                        datasets.add(descriptor);
                    }
                }
            }
        }

        return datasets;
    }

    private DatasetDescriptor parseNode(FlowDefinition.Node node) {
        if (node.appData() == null || node.appData().properties() == null) {
            return null;
        }

        var props = node.appData().properties();
        ConnectorType connectorType = ConnectorType.fromNodeTypeId(node.nodeTypeId());

        String datasetName = resolveDatasetName(props, connectorType);
        if (datasetName == null) {
            return null;
        }

        String namespace = resolveNamespace(props, connectorType);
        DatasetDescriptor.Type type = Boolean.TRUE.equals(props.isSource())
                ? DatasetDescriptor.Type.INPUT
                : DatasetDescriptor.Type.OUTPUT;

        return new DatasetDescriptor(namespace, datasetName, type);
    }

    private String resolveDatasetName(FlowDefinition.Properties props, ConnectorType connectorType) {
        if (props.tableName() != null && !props.tableName().isBlank()) {
            return props.tableName();
        }
        if (props.filePath() != null && !props.filePath().isBlank()) {
            return props.filePath();
        }
        return null;
    }

    private String resolveNamespace(FlowDefinition.Properties props, ConnectorType connectorType) {
        if (props.connectionRef() != null && !props.connectionRef().isBlank()) {
            return connectionNamespaceCache.computeIfAbsent(props.connectionRef(),
                    this::fetchConnectionNamespace);
        }
        return connectorType.getNamespaceScheme() + "://unknown";
    }

    private String fetchConnectionNamespace(String connectionId) {
        try {
            ConnectionDetail detail = client.getConnection(connectionId);
            if (detail != null && detail.entity() != null && detail.entity().properties() != null) {
                var connProps = detail.entity().properties();

                if (connProps.url() != null && !connProps.url().isBlank()) {
                    return connProps.url();
                }

                String connType = connProps.connectionType() != null
                        ? connProps.connectionType().toLowerCase() : "unknown";
                String host = connProps.host() != null ? connProps.host() : "unknown";
                Integer port = connProps.port();
                String database = connProps.database();

                StringBuilder ns = new StringBuilder(connType).append("://").append(host);
                if (port != null) {
                    ns.append(":").append(port);
                }
                if (database != null && !database.isBlank()) {
                    ns.append("/").append(database);
                }
                return ns.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to resolve connection namespace for {}", connectionId, e);
        }
        return "unknown://" + connectionId;
    }
}
