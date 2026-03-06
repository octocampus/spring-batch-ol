package io.openlineage.datastage.dsjob.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DsjobRunner {

    private static final Logger log = LoggerFactory.getLogger(DsjobRunner.class);
    private static final long TIMEOUT_SECONDS = 60;

    // dsjob -jobinfo output patterns
    private static final Pattern STATUS_PATTERN = Pattern.compile(
            "Job Status\\s*:\\s*(.+?)\\s*\\((\\d+)\\)");
    private static final Pattern START_TIME_PATTERN = Pattern.compile(
            "(?:Last Run Start|Job Start Time)\\s*:\\s*(.+)");
    private static final Pattern END_TIME_PATTERN = Pattern.compile(
            "(?:Last Run End|Job End Time)\\s*:\\s*(.+)");
    private static final Pattern WAVE_PATTERN = Pattern.compile(
            "Job Wave No\\s*:\\s*(\\d+)");

    // dsjob -report output patterns
    private static final Pattern STAGE_HEADER_PATTERN = Pattern.compile(
            "^=*\\s*Stage:\\s*(\\S+)");
    private static final Pattern ROWS_READ_PATTERN = Pattern.compile(
            "Rows Read\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ROWS_WRITTEN_PATTERN = Pattern.compile(
            "Rows Written\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    private final String dsHome;

    public DsjobRunner(String dsHome) {
        this.dsHome = dsHome;
    }

    public List<String> listJobs(String project) {
        String output = execute("-ljobs", project);
        if (output == null) return List.of();

        List<String> jobs = new ArrayList<>();
        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                jobs.add(trimmed);
            }
        }
        return jobs;
    }

    public JobInfo getJobInfo(String project, String jobName) {
        String output = execute("-jobinfo", project, jobName);
        if (output == null) return null;

        int statusCode = -1;
        String statusText = "Unknown";
        String startTime = null;
        String endTime = null;
        int waveNumber = 0;

        for (String line : output.split("\n")) {
            Matcher m;

            m = STATUS_PATTERN.matcher(line);
            if (m.find()) {
                statusText = m.group(1).trim();
                statusCode = Integer.parseInt(m.group(2));
                continue;
            }

            m = START_TIME_PATTERN.matcher(line);
            if (m.find()) {
                startTime = m.group(1).trim();
                continue;
            }

            m = END_TIME_PATTERN.matcher(line);
            if (m.find()) {
                endTime = m.group(1).trim();
                continue;
            }

            m = WAVE_PATTERN.matcher(line);
            if (m.find()) {
                waveNumber = Integer.parseInt(m.group(1));
            }
        }

        if (statusCode == -1) {
            log.warn("Could not parse jobinfo output for {}/{}", project, jobName);
            return null;
        }

        return new JobInfo(statusCode, statusText, startTime, endTime, waveNumber);
    }

    public JobReport getJobReport(String project, String jobName) {
        String output = execute("-report", project, jobName);
        if (output == null) return new JobReport(List.of());

        List<JobReport.StageReport> stages = new ArrayList<>();
        String currentStage = null;
        long rowsRead = 0;
        long rowsWritten = 0;

        for (String line : output.split("\n")) {
            Matcher stageMatch = STAGE_HEADER_PATTERN.matcher(line);
            if (stageMatch.find()) {
                if (currentStage != null) {
                    stages.add(new JobReport.StageReport(currentStage, rowsRead, rowsWritten));
                }
                currentStage = stageMatch.group(1);
                rowsRead = 0;
                rowsWritten = 0;
                continue;
            }

            Matcher readMatch = ROWS_READ_PATTERN.matcher(line);
            if (readMatch.find()) {
                rowsRead += Long.parseLong(readMatch.group(1));
                continue;
            }

            Matcher writeMatch = ROWS_WRITTEN_PATTERN.matcher(line);
            if (writeMatch.find()) {
                rowsWritten += Long.parseLong(writeMatch.group(1));
            }
        }

        if (currentStage != null) {
            stages.add(new JobReport.StageReport(currentStage, rowsRead, rowsWritten));
        }

        return new JobReport(stages);
    }

    public List<String> listStages(String project, String jobName) {
        String output = execute("-lstages", project, jobName);
        if (output == null) return List.of();

        List<String> stages = new ArrayList<>();
        for (String line : output.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                stages.add(trimmed);
            }
        }
        return stages;
    }

    private String execute(String... args) {
        try {
            String dsjobPath = Path.of(dsHome, "bin", "dsjob").toString();

            List<String> command = new ArrayList<>();
            command.add(dsjobPath);
            for (String arg : args) {
                command.add(arg);
            }

            log.debug("Executing: {}", String.join(" ", command));

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.environment().put("DSHOME", dsHome);
            pb.redirectErrorStream(true);

            Process process = pb.start();
            StringBuilder output = new StringBuilder();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            boolean finished = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.warn("dsjob command timed out: {}", String.join(" ", command));
                return null;
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                log.warn("dsjob exited with code {} for: {} — output: {}",
                        exitCode, String.join(" ", command), output);
                return null;
            }

            return output.toString();
        } catch (Exception e) {
            log.warn("Failed to execute dsjob command", e);
            return null;
        }
    }
}
