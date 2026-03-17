package io.openlineage.datastage.dsodb.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Timestamp;
import java.util.List;

public class DsodbJobRepository {

    private static final Logger log = LoggerFactory.getLogger(DsodbJobRepository.class);

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public DsodbJobRepository(NamedParameterJdbcTemplate jdbc, String schema) {
        this.jdbc = jdbc;
        this.schema = schema;
    }

    public List<JobExecRow> findChangedRuns(List<String> projects, Timestamp since) {
        String sql = "SELECT JOBNAME, PROJECTNAME, WAVENO, JOBSTATUS, RUNSTARTTIME, RUNENDTIME"
                + " FROM " + schema + ".JOBEXEC"
                + " WHERE PROJECTNAME IN (:projects)"
                + " AND (RUNSTARTTIME > :since OR RUNENDTIME > :since OR JOBSTATUS = 1)"
                + " ORDER BY RUNSTARTTIME ASC";

        var params = new MapSqlParameterSource()
                .addValue("projects", projects)
                .addValue("since", since);

        return jdbc.query(sql, params, (rs, rowNum) -> new JobExecRow(
                rs.getString("JOBNAME"),
                rs.getString("PROJECTNAME"),
                rs.getInt("WAVENO"),
                rs.getInt("JOBSTATUS"),
                rs.getTimestamp("RUNSTARTTIME"),
                rs.getTimestamp("RUNENDTIME")));
    }

    public List<StageResultRow> findStageResults(String jobName, int waveNo) {
        String sql = "SELECT JOBNAME, STAGENAME, WAVENO, ROWSREAD, ROWSWRITTEN, ROWSREJECTED"
                + " FROM " + schema + ".STAGERESULT"
                + " WHERE JOBNAME = :jobName AND WAVENO = :waveNo";

        var params = new MapSqlParameterSource()
                .addValue("jobName", jobName)
                .addValue("waveNo", waveNo);

        return jdbc.query(sql, params, (rs, rowNum) -> new StageResultRow(
                rs.getString("JOBNAME"),
                rs.getString("STAGENAME"),
                rs.getInt("WAVENO"),
                rs.getLong("ROWSREAD"),
                rs.getLong("ROWSWRITTEN"),
                rs.getLong("ROWSREJECTED")));
    }

    public List<LinkResultRow> findLinkResults(String jobName, int waveNo) {
        String sql = "SELECT JOBNAME, STAGENAME, LINKNAME, WAVENO, ROWCOUNT"
                + " FROM " + schema + ".LINKRESULT"
                + " WHERE JOBNAME = :jobName AND WAVENO = :waveNo";

        var params = new MapSqlParameterSource()
                .addValue("jobName", jobName)
                .addValue("waveNo", waveNo);

        return jdbc.query(sql, params, (rs, rowNum) -> new LinkResultRow(
                rs.getString("JOBNAME"),
                rs.getString("STAGENAME"),
                rs.getString("LINKNAME"),
                rs.getInt("WAVENO"),
                rs.getLong("ROWCOUNT")));
    }
}
