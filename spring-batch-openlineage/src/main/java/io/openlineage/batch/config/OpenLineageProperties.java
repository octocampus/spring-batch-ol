package io.openlineage.batch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "openlineage")
public class OpenLineageProperties {

    private boolean enabled = true;
    private String namespace = "spring-batch";
    private TransportType transport = TransportType.CONSOLE;
    private Http http = new Http();
    private Granularity granularity = new Granularity();

    public enum TransportType {
        CONSOLE, HTTP, NOOP
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public TransportType getTransport() {
        return transport;
    }

    public void setTransport(TransportType transport) {
        this.transport = transport;
    }

    public Http getHttp() {
        return http;
    }

    public void setHttp(Http http) {
        this.http = http;
    }

    public Granularity getGranularity() {
        return granularity;
    }

    public void setGranularity(Granularity granularity) {
        this.granularity = granularity;
    }

    public static class Http {
        private String url;
        private String apiKey;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }
    }

    public static class Granularity {
        private boolean jobEvents = true;
        private boolean stepEvents = true;
        private boolean chunkEvents = true;
        private int chunkEventInterval = 1;

        public boolean isJobEvents() {
            return jobEvents;
        }

        public void setJobEvents(boolean jobEvents) {
            this.jobEvents = jobEvents;
        }

        public boolean isStepEvents() {
            return stepEvents;
        }

        public void setStepEvents(boolean stepEvents) {
            this.stepEvents = stepEvents;
        }

        public boolean isChunkEvents() {
            return chunkEvents;
        }

        public void setChunkEvents(boolean chunkEvents) {
            this.chunkEvents = chunkEvents;
        }

        public int getChunkEventInterval() {
            return chunkEventInterval;
        }

        public void setChunkEventInterval(int chunkEventInterval) {
            this.chunkEventInterval = chunkEventInterval;
        }
    }
}
