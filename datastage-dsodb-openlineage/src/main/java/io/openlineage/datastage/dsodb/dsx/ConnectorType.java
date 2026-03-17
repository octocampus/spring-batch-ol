package io.openlineage.datastage.dsodb.dsx;

import java.util.Map;

public enum ConnectorType {

    DB2("db2"), ORACLE("oracle"), POSTGRES("postgresql"), MYSQL("mysql"),
    SQL_SERVER("sqlserver"), TERADATA("teradata"), NETEZZA("netezza"), ODBC("odbc"),
    SEQUENTIAL_FILE("file"), DATASET("file"), UNKNOWN("unknown");

    private final String namespaceScheme;

    ConnectorType(String namespaceScheme) { this.namespaceScheme = namespaceScheme; }

    public String getNamespaceScheme() { return namespaceScheme; }

    private static final Map<String, ConnectorType> STAGE_TYPE_MAPPING = Map.ofEntries(
            Map.entry("db2connectorpx", DB2), Map.entry("pxdb2", DB2),
            Map.entry("oracleconnectorpx", ORACLE), Map.entry("pxoracle", ORACLE),
            Map.entry("teradataconnectorpx", TERADATA), Map.entry("pxteradata", TERADATA),
            Map.entry("netezzaconnectorpx", NETEZZA), Map.entry("pxnetezza", NETEZZA),
            Map.entry("odbcconnectorpx", ODBC), Map.entry("pxodbc", ODBC),
            Map.entry("mysqlconnectorpx", MYSQL), Map.entry("postgresqlconnectorpx", POSTGRES),
            Map.entry("sqlserverconnectorpx", SQL_SERVER),
            Map.entry("pxsequentialfile", SEQUENTIAL_FILE), Map.entry("pxdataset", DATASET),
            Map.entry("cdb2stage", DB2), Map.entry("cuniloaddstage", DB2),
            Map.entry("coraclestage", ORACLE), Map.entry("codbcstage", ODBC),
            Map.entry("cteradatastage", TERADATA), Map.entry("cnetezzastage", NETEZZA),
            Map.entry("csequentialfilestage", SEQUENTIAL_FILE), Map.entry("cdatasetstage", DATASET));

    public static ConnectorType fromStageType(String stageType) {
        if (stageType == null) return UNKNOWN;
        return STAGE_TYPE_MAPPING.getOrDefault(stageType.toLowerCase(), UNKNOWN);
    }
}
