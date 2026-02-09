package io.openlineage.batch.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.AbstractCursorItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcDatasetExtractor implements DatasetExtractor {

    private static final Logger log = LoggerFactory.getLogger(JdbcDatasetExtractor.class);

    private static final Pattern FROM_PATTERN = Pattern.compile(
            "\\bFROM\\s+([\\w.`\"\\[\\]]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_PATTERN = Pattern.compile(
            "\\bINSERT\\s+INTO\\s+([\\w.`\"\\[\\]]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile(
            "\\bUPDATE\\s+([\\w.`\"\\[\\]]+)", Pattern.CASE_INSENSITIVE);

    @Override
    public boolean supports(Object component) {
        return component instanceof AbstractCursorItemReader<?>
                || component instanceof JdbcBatchItemWriter<?>
                || component instanceof JdbcPagingItemReader<?>;
    }

    @Override
    public DatasetInfo extract(Object component) {
        try {
            if (component instanceof AbstractCursorItemReader<?> reader) {
                return extractFromCursorReader(reader);
            } else if (component instanceof JdbcPagingItemReader<?> reader) {
                return extractFromPagingReader(reader);
            } else if (component instanceof JdbcBatchItemWriter<?> writer) {
                return extractFromWriter(writer);
            }
        } catch (Exception e) {
            log.warn("Failed to extract JDBC dataset info from {}", component.getClass().getSimpleName(), e);
        }
        return DatasetInfo.unknown(DatasetInfo.Type.INPUT);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private DatasetInfo extractFromCursorReader(AbstractCursorItemReader<?> reader) {
        DataSource ds = getField(reader, "dataSource", DataSource.class);
        String sql = getField(reader, "sql", String.class);
        String jdbcUrl = resolveJdbcUrl(ds);
        String table = parseTableName(sql, FROM_PATTERN);
        return new DatasetInfo(jdbcUrl, table, DatasetInfo.Type.INPUT);
    }

    private DatasetInfo extractFromPagingReader(JdbcPagingItemReader<?> reader) {
        DataSource ds = getField(reader, "dataSource", DataSource.class);
        String jdbcUrl = resolveJdbcUrl(ds);
        return new DatasetInfo(jdbcUrl, "UNKNOWN", DatasetInfo.Type.INPUT);
    }

    private DatasetInfo extractFromWriter(JdbcBatchItemWriter<?> writer) {
        DataSource ds = resolveDataSourceFromWriter(writer);
        String sql = getField(writer, "sql", String.class);
        String jdbcUrl = resolveJdbcUrl(ds);
        String table = parseTableName(sql, INSERT_PATTERN);
        if ("UNKNOWN".equals(table)) {
            table = parseTableName(sql, UPDATE_PATTERN);
        }
        return new DatasetInfo(jdbcUrl, table, DatasetInfo.Type.OUTPUT);
    }

    private DataSource resolveDataSourceFromWriter(JdbcBatchItemWriter<?> writer) {
        // JdbcBatchItemWriter stores a NamedParameterJdbcTemplate, not a raw DataSource
        try {
            Object template = getField(writer, "namedParameterJdbcTemplate", Object.class);
            if (template != null) {
                // NamedParameterJdbcTemplate -> getJdbcOperations() -> JdbcTemplate
                java.lang.reflect.Method getJdbcOps = template.getClass().getMethod("getJdbcOperations");
                Object jdbcTemplate = getJdbcOps.invoke(template);
                if (jdbcTemplate != null) {
                    java.lang.reflect.Method getDs = jdbcTemplate.getClass().getMethod("getDataSource");
                    return (DataSource) getDs.invoke(jdbcTemplate);
                }
            }
        } catch (Exception e) {
            log.debug("Could not resolve DataSource from JdbcBatchItemWriter via template", e);
        }
        // Fallback to direct field
        return getField(writer, "dataSource", DataSource.class);
    }

    private String resolveJdbcUrl(DataSource ds) {
        if (ds == null) {
            return "jdbc:unknown";
        }
        // Try HikariCP getJdbcUrl() method
        try {
            java.lang.reflect.Method m = ds.getClass().getMethod("getJdbcUrl");
            String url = (String) m.invoke(ds);
            if (url != null) return url;
        } catch (Exception ignored) {
        }
        // Try HikariCP field
        try {
            String url = getField(ds, "jdbcUrl", String.class);
            if (url != null) return url;
        } catch (Exception ignored) {
        }
        // Try generic getUrl() method
        try {
            java.lang.reflect.Method m = ds.getClass().getMethod("getUrl");
            String url = (String) m.invoke(ds);
            if (url != null) return url;
        } catch (Exception ignored) {
        }
        // Fallback: get connection metadata
        try (Connection conn = ds.getConnection()) {
            return conn.getMetaData().getURL();
        } catch (Exception e) {
            log.debug("Could not resolve JDBC URL from DataSource", e);
            return "jdbc:unknown";
        }
    }

    private String parseTableName(String sql, Pattern pattern) {
        if (sql == null) return "UNKNOWN";
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return matcher.group(1).replaceAll("[`\"\\[\\]]", "");
        }
        return "UNKNOWN";
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName, Class<T> type) {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
