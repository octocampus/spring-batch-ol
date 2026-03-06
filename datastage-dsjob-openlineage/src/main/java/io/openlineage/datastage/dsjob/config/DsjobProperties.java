package io.openlineage.datastage.dsjob.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "datastage")
public class DsjobProperties {

    private String dsHome = "/opt/IBM/InformationServer/Server/DSEngine";
    private List<String> projects = List.of();
    private String dsxDirectory;
    private Polling polling = new Polling();

    public String getDsHome() {
        return dsHome;
    }

    public void setDsHome(String dsHome) {
        this.dsHome = dsHome;
    }

    public List<String> getProjects() {
        return projects;
    }

    public void setProjects(List<String> projects) {
        this.projects = projects;
    }

    public String getDsxDirectory() {
        return dsxDirectory;
    }

    public void setDsxDirectory(String dsxDirectory) {
        this.dsxDirectory = dsxDirectory;
    }

    public Polling getPolling() {
        return polling;
    }

    public void setPolling(Polling polling) {
        this.polling = polling;
    }

    public static class Polling {
        private Duration interval = Duration.ofSeconds(30);
        private Duration runRetention = Duration.ofHours(2);

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration interval) {
            this.interval = interval;
        }

        public Duration getRunRetention() {
            return runRetention;
        }

        public void setRunRetention(Duration runRetention) {
            this.runRetention = runRetention;
        }
    }
}
