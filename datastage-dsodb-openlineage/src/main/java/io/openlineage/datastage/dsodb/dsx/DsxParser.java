package io.openlineage.datastage.dsodb.dsx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DsxParser {

    private static final Logger log = LoggerFactory.getLogger(DsxParser.class);

    private static final Pattern KV_PATTERN = Pattern.compile("^\\s*(\\w+)\\s+\"(.*)\"\\s*$");
    private static final Pattern TABLE_PATTERN = Pattern.compile(
            "(?:TableName|TABLE|Table|table_name|TABLENAME)\\s*[\"=>]+\\s*([^\"<>]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "(?:FileName|FILE|FilePath|file_path|FILENAME|Directory)\\s*[\"=>]+\\s*([^\"<>]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SERVER_PATTERN = Pattern.compile(
            "(?:Server|ServerName|HOST|HostName|server)\\s*[\"=>]+\\s*([^\"<>]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DATABASE_PATTERN = Pattern.compile(
            "(?:Database|DatabaseName|DBNAME|database|SID)\\s*[\"=>]+\\s*([^\"<>]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PORT_PATTERN = Pattern.compile(
            "(?:Port|PORT|PortNumber|port)\\s*[\"=>]+\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    public Map<String, List<DatasetDescriptor>> parseDirectory(Path directory) {
        Map<String, List<DatasetDescriptor>> result = new HashMap<>();
        if (directory == null || !Files.isDirectory(directory)) {
            log.warn("DSX directory does not exist or is not a directory: {}", directory);
            return result;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.dsx")) {
            for (Path file : stream) {
                log.debug("Parsing DSX file: {}", file);
                parseFile(file).forEach((jobName, datasets) ->
                        result.merge(jobName, datasets, (a, b) -> {
                            List<DatasetDescriptor> merged = new ArrayList<>(a);
                            merged.addAll(b);
                            return merged;
                        }));
            }
        } catch (IOException e) {
            log.warn("Failed to scan DSX directory: {}", directory, e);
        }
        log.info("Parsed {} jobs with dataset info from DSX files in {}", result.size(), directory);
        return result;
    }

    Map<String, List<DatasetDescriptor>> parseFile(Path file) {
        Map<String, List<DatasetDescriptor>> result = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            String currentJobName = null;
            List<DatasetDescriptor> currentDatasets = null;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.equals("BEGIN DSJOB")) {
                    currentJobName = null;
                    currentDatasets = new ArrayList<>();
                } else if (trimmed.equals("END DSJOB")) {
                    if (currentJobName != null && currentDatasets != null && !currentDatasets.isEmpty()) {
                        result.put(currentJobName, currentDatasets);
                    }
                    currentJobName = null;
                    currentDatasets = null;
                } else if (currentDatasets != null && trimmed.equals("BEGIN DSRECORD")) {
                    Map<String, String> record = readRecord(reader);
                    if ("CJobDefn".equals(record.get("OLEType"))) {
                        currentJobName = record.get("Name");
                    } else {
                        ConnectorType connType = ConnectorType.fromStageType(record.get("StageType"));
                        if (connType != ConnectorType.UNKNOWN) {
                            DatasetDescriptor desc = buildDescriptor(record, connType);
                            if (desc != null) currentDatasets.add(desc);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to parse DSX file: {}", file, e);
        }
        return result;
    }

    private Map<String, String> readRecord(BufferedReader reader) throws IOException {
        Map<String, String> fields = new HashMap<>();
        StringBuilder propsAccum = new StringBuilder();
        int depth = 1;
        String line;
        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.startsWith("BEGIN ")) { depth++; continue; }
            if (trimmed.startsWith("END ")) { depth--; if (depth == 0) break; continue; }
            Matcher m = KV_PATTERN.matcher(line);
            if (m.matches()) {
                String key = m.group(1), value = m.group(2);
                if ("Properties".equals(key) || "XMLProperties".equals(key)) {
                    propsAccum.append(value).append(" ");
                } else {
                    fields.putIfAbsent(key, value);
                }
            }
        }
        if (!propsAccum.isEmpty()) fields.put("Properties", propsAccum.toString());
        return fields;
    }

    private DatasetDescriptor buildDescriptor(Map<String, String> record, ConnectorType connType) {
        String stageName = record.get("Name");
        String properties = record.getOrDefault("Properties", "");
        boolean isSource = "0".equals(record.getOrDefault("InputPins", "0"))
                && !"0".equals(record.getOrDefault("OutputPins", "0"));
        DatasetDescriptor.Type type = isSource ? DatasetDescriptor.Type.INPUT : DatasetDescriptor.Type.OUTPUT;
        String datasetName = extractDatasetName(properties, connType);
        if (datasetName == null) datasetName = stageName;
        return new DatasetDescriptor(buildNamespace(properties, connType), datasetName, type, stageName);
    }

    private String extractDatasetName(String props, ConnectorType connType) {
        if (connType == ConnectorType.SEQUENTIAL_FILE || connType == ConnectorType.DATASET) {
            Matcher m = FILE_PATTERN.matcher(props);
            if (m.find()) return m.group(1).trim();
        }
        Matcher m = TABLE_PATTERN.matcher(props);
        if (m.find()) return m.group(1).trim();
        m = FILE_PATTERN.matcher(props);
        if (m.find()) return m.group(1).trim();
        return null;
    }

    private String buildNamespace(String props, ConnectorType connType) {
        String scheme = connType.getNamespaceScheme();
        Matcher sm = SERVER_PATTERN.matcher(props);
        String server = sm.find() ? sm.group(1).trim() : null;
        if (server == null) return scheme + "://localhost";
        StringBuilder ns = new StringBuilder(scheme).append("://").append(server);
        Matcher pm = PORT_PATTERN.matcher(props);
        if (pm.find()) ns.append(":").append(pm.group(1).trim());
        Matcher dm = DATABASE_PATTERN.matcher(props);
        if (dm.find()) ns.append("/").append(dm.group(1).trim());
        return ns.toString();
    }
}
