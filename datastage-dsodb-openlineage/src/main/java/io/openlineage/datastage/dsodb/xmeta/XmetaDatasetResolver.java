package io.openlineage.datastage.dsodb.xmeta;

import io.openlineage.datastage.dsodb.dsx.ConnectorType;
import io.openlineage.datastage.dsodb.dsx.DatasetDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

public class XmetaDatasetResolver {

    private static final Logger log = LoggerFactory.getLogger(XmetaDatasetResolver.class);

    private final JdbcTemplate jdbc;
    private final String schema;

    public XmetaDatasetResolver(JdbcTemplate jdbc, String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
    }

    public List<DatasetDescriptor> resolveDatasets(String projectName, String jobName) {
        try {
            String sql = "SELECT s.STAGENAME, s.STAGETYPE,"
                    + " (SELECT COUNT(*) FROM " + schema + ".DSLINK l"
                    + "  WHERE l.JOBNAME = s.JOBNAME AND l.PROJECTNAME = s.PROJECTNAME"
                    + "  AND l.TARGETSTAGE = s.STAGENAME) AS INPUT_LINKS,"
                    + " (SELECT COUNT(*) FROM " + schema + ".DSLINK l"
                    + "  WHERE l.JOBNAME = s.JOBNAME AND l.PROJECTNAME = s.PROJECTNAME"
                    + "  AND l.SOURCESTAGE = s.STAGENAME) AS OUTPUT_LINKS,"
                    + " s.TABLENAME,"
                    + " c.HOSTNAME AS SERVER, c.PORTNUMBER AS PORT,"
                    + " c.DATABASENAME AS DATABASE, c.CONNECTIONTYPE"
                    + " FROM " + schema + ".DSSTAGE s"
                    + " LEFT JOIN " + schema + ".DSCONNECTION c"
                    + "  ON s.CONNECTIONNAME = c.CONNECTIONNAME"
                    + "  AND s.PROJECTNAME = c.PROJECTNAME"
                    + " WHERE s.JOBNAME = ? AND s.PROJECTNAME = ?";

            List<XmetaStageRow> rows = jdbc.query(sql, (rs, rowNum) -> new XmetaStageRow(
                    jobName,
                    rs.getString("STAGENAME"),
                    rs.getString("STAGETYPE"),
                    rs.getInt("INPUT_LINKS"),
                    rs.getInt("OUTPUT_LINKS"),
                    rs.getString("TABLENAME"),
                    rs.getString("SERVER"),
                    rs.getString("PORT"),
                    rs.getString("DATABASE"),
                    rs.getString("CONNECTIONTYPE")),
                    jobName, projectName);

            List<DatasetDescriptor> datasets = new ArrayList<>();
            for (XmetaStageRow row : rows) {
                ConnectorType connType = ConnectorType.fromStageType(row.stageType());
                if (connType == ConnectorType.UNKNOWN) continue;

                boolean isSource = row.inputLinks() == 0 && row.outputLinks() > 0;
                DatasetDescriptor.Type type = isSource
                        ? DatasetDescriptor.Type.INPUT
                        : DatasetDescriptor.Type.OUTPUT;

                String datasetName = row.tableName() != null && !row.tableName().isBlank()
                        ? row.tableName() : row.stageName();

                String namespace = buildNamespace(row, connType);
                datasets.add(new DatasetDescriptor(namespace, datasetName, type, row.stageName()));
            }

            return datasets;
        } catch (Exception e) {
            log.warn("Failed to resolve datasets from xmeta for {}/{}", projectName, jobName, e);
            return List.of();
        }
    }

    private String buildNamespace(XmetaStageRow row, ConnectorType connType) {
        String scheme = connType.getNamespaceScheme();
        String server = row.server();
        if (server == null || server.isBlank()) return scheme + "://localhost";

        StringBuilder ns = new StringBuilder(scheme).append("://").append(server);
        if (row.port() != null && !row.port().isBlank()) ns.append(":").append(row.port());
        if (row.database() != null && !row.database().isBlank()) ns.append("/").append(row.database());
        return ns.toString();
    }
}
