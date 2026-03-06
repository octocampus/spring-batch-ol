package io.openlineage.datastage.dsjob.dsx;

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

/**
 * Parses DataStage .dsx export files to extract dataset information (table names,
 * file paths, connection details) from connector stages.
 *
 * DSX format structure:
 * <pre>
 * BEGIN DSJOB
 *    Name "job_name"
 *    BEGIN DSRECORD
 *       Name "stage_name"
 *       StageType "DB2ConnectorPX"
 *       OLEType "CCustomStage"
 *       InputPins "0"
 *       OutputPins "1"
 *       Properties "...xml or key=value..."
 *    END DSRECORD
 * END DSJOB
 * </pre>
 */
public class DsxParser {

    private static final Logger log = LoggerFactory.getLogger(DsxParser.class);

    private static final Pattern KV_PATTERN = Pattern.compile(
            "^\\s*(\\w+)\\s+\"(.*)\"\\s*$");

    // Property patterns for extracting dataset info from the Properties XML/text blob
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

    /**
     * Scans a directory for .dsx files and parses all of them.
     * @return map of job name → list of dataset descriptors
     */
    public Map<String, List<DatasetDescriptor>> parseDirectory(Path directory) {
        Map<String, List<DatasetDescriptor>> result = new HashMap<>();

        if (directory == null || !Files.isDirectory(directory)) {
            log.warn("DSX directory does not exist or is not a directory: {}", directory);
            return result;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.dsx")) {
            for (Path file : stream) {
                log.debug("Parsing DSX file: {}", file);
                Map<String, List<DatasetDescriptor>> parsed = parseFile(file);
                parsed.forEach((jobName, datasets) ->
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

    /**
     * Parses a single .dsx file.
     * @return map of job name → list of dataset descriptors
     */
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
                    continue;
                }

                if (trimmed.equals("END DSJOB")) {
                    if (currentJobName != null && currentDatasets != null && !currentDatasets.isEmpty()) {
                        result.put(currentJobName, currentDatasets);
                    }
                    currentJobName = null;
                    currentDatasets = null;
                    continue;
                }

                if (currentDatasets == null) continue;

                if (trimmed.equals("BEGIN DSRECORD")) {
                    Map<String, String> record = readRecord(reader);
                    String oleType = record.get("OLEType");

                    // The job-level record has OLEType CJobDefn — extract job name from it
                    if ("CJobDefn".equals(oleType)) {
                        currentJobName = record.get("Name");
                        continue;
                    }

                    // Only process connector stages, skip transformers
                    String stageType = record.get("StageType");
                    ConnectorType connType = ConnectorType.fromStageType(stageType);
                    if (connType == ConnectorType.UNKNOWN) continue;

                    DatasetDescriptor descriptor = buildDescriptor(record, connType);
                    if (descriptor != null) {
                        currentDatasets.add(descriptor);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to parse DSX file: {}", file, e);
        }

        return result;
    }

    /**
     * Reads a DSRECORD block (key-value pairs + nested DSSUBRECORD blocks).
     * Accumulates all Properties values since they may span sub-records.
     */
    private Map<String, String> readRecord(BufferedReader reader) throws IOException {
        Map<String, String> fields = new HashMap<>();
        StringBuilder propertiesAccum = new StringBuilder();
        int depth = 1;
        String line;

        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();

            if (trimmed.startsWith("BEGIN ")) {
                depth++;
                continue;
            }
            if (trimmed.startsWith("END ")) {
                depth--;
                if (depth == 0) break;
                continue;
            }

            Matcher m = KV_PATTERN.matcher(line);
            if (m.matches()) {
                String key = m.group(1);
                String value = m.group(2);

                if ("Properties".equals(key) || "XMLProperties".equals(key)) {
                    propertiesAccum.append(value).append(" ");
                } else {
                    fields.putIfAbsent(key, value);
                }
            }
        }

        if (!propertiesAccum.isEmpty()) {
            fields.put("Properties", propertiesAccum.toString());
        }

        return fields;
    }

    private DatasetDescriptor buildDescriptor(Map<String, String> record, ConnectorType connType) {
        String stageName = record.get("Name");
        String properties = record.getOrDefault("Properties", "");
        String inputPins = record.getOrDefault("InputPins", "0");
        String outputPins = record.getOrDefault("OutputPins", "0");

        // Determine INPUT (source) vs OUTPUT (target)
        boolean isSource = "0".equals(inputPins) && !"0".equals(outputPins);
        DatasetDescriptor.Type type = isSource
                ? DatasetDescriptor.Type.INPUT
                : DatasetDescriptor.Type.OUTPUT;

        // Extract dataset name (table or file)
        String datasetName = extractDatasetName(properties, connType);
        if (datasetName == null) {
            datasetName = stageName;
        }

        // Build namespace from connection info
        String namespace = buildNamespace(properties, connType);

        return new DatasetDescriptor(namespace, datasetName, type, stageName);
    }

    private String extractDatasetName(String properties, ConnectorType connType) {
        if (connType == ConnectorType.SEQUENTIAL_FILE || connType == ConnectorType.DATASET) {
            Matcher m = FILE_PATTERN.matcher(properties);
            if (m.find()) return m.group(1).trim();
        }

        Matcher m = TABLE_PATTERN.matcher(properties);
        if (m.find()) return m.group(1).trim();

        m = FILE_PATTERN.matcher(properties);
        if (m.find()) return m.group(1).trim();

        return null;
    }

    private String buildNamespace(String properties, ConnectorType connType) {
        String scheme = connType.getNamespaceScheme();

        Matcher serverMatch = SERVER_PATTERN.matcher(properties);
        String server = serverMatch.find() ? serverMatch.group(1).trim() : null;

        Matcher portMatch = PORT_PATTERN.matcher(properties);
        String port = portMatch.find() ? portMatch.group(1).trim() : null;

        Matcher dbMatch = DATABASE_PATTERN.matcher(properties);
        String database = dbMatch.find() ? dbMatch.group(1).trim() : null;

        if (server == null) {
            return scheme + "://localhost";
        }

        StringBuilder ns = new StringBuilder(scheme).append("://").append(server);
        if (port != null) {
            ns.append(":").append(port);
        }
        if (database != null) {
            ns.append("/").append(database);
        }
        return ns.toString();
    }
}
