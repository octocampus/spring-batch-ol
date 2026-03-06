package io.openlineage.datastage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "datastage")
public class DataStageProperties {

    private String apiKey;
    private String serviceUrl = "https://api.dataplatform.cloud.ibm.com";
    private String projectId;
    private Polling polling = new Polling();

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
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
