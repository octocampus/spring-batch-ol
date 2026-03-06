package io.openlineage.datastage.flow;

import java.util.Map;

public enum ConnectorType {

    DB2("db2"),
    ORACLE("oracle"),
    POSTGRES("postgresql"),
    MYSQL("mysql"),
    SQL_SERVER("sqlserver"),
    FILE("file"),
    S3("s3"),
    CLOUD_OBJECT_STORAGE("cos"),
    UNKNOWN("unknown");

    private final String namespaceScheme;

    ConnectorType(String namespaceScheme) {
        this.namespaceScheme = namespaceScheme;
    }

    public String getNamespaceScheme() {
        return namespaceScheme;
    }

    private static final Map<String, ConnectorType> NODE_TYPE_MAPPING = Map.ofEntries(
            Map.entry("com.ibm.datastage.connector.db2", DB2),
            Map.entry("com.ibm.datastage.connector.oracle", ORACLE),
            Map.entry("com.ibm.datastage.connector.postgres", POSTGRES),
            Map.entry("com.ibm.datastage.connector.postgresql", POSTGRES),
            Map.entry("com.ibm.datastage.connector.mysql", MYSQL),
            Map.entry("com.ibm.datastage.connector.sqlserver", SQL_SERVER),
            Map.entry("com.ibm.datastage.connector.localfileconnector", FILE),
            Map.entry("com.ibm.datastage.connector.fileconnector", FILE),
            Map.entry("com.ibm.datastage.connector.amazons3", S3),
            Map.entry("com.ibm.datastage.connector.cloudobjectstorage", CLOUD_OBJECT_STORAGE)
    );

    public static ConnectorType fromNodeTypeId(String nodeTypeId) {
        if (nodeTypeId == null) {
            return UNKNOWN;
        }
        return NODE_TYPE_MAPPING.getOrDefault(nodeTypeId.toLowerCase(), UNKNOWN);
    }
}
