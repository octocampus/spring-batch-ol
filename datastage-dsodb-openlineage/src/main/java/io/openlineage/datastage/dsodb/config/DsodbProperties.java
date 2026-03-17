package io.openlineage.datastage.dsodb.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "datastage")
public class DsodbProperties {

    private Dsodb dsodb = new Dsodb();
    private Xmeta xmeta = new Xmeta();
    private List<String> projects = List.of();
    private String dsxDirectory;
    private Polling polling = new Polling();

    public Dsodb getDsodb() { return dsodb; }
    public void setDsodb(Dsodb dsodb) { this.dsodb = dsodb; }
    public Xmeta getXmeta() { return xmeta; }
    public void setXmeta(Xmeta xmeta) { this.xmeta = xmeta; }
    public List<String> getProjects() { return projects; }
    public void setProjects(List<String> projects) { this.projects = projects; }
    public String getDsxDirectory() { return dsxDirectory; }
    public void setDsxDirectory(String dsxDirectory) { this.dsxDirectory = dsxDirectory; }
    public Polling getPolling() { return polling; }
    public void setPolling(Polling polling) { this.polling = polling; }

    public static class Dsodb {
        private String url;
        private String username;
        private String password;
        private String driverClassName = "com.ibm.db2.jcc.DB2Driver";
        private String schema = "DSODB";

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDriverClassName() { return driverClassName; }
        public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }
    }

    public static class Xmeta {
        private boolean enabled = false;
        private String url;
        private String username;
        private String password;
        private String driverClassName = "com.ibm.db2.jcc.DB2Driver";
        private String schema = "IAVIEWS";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDriverClassName() { return driverClassName; }
        public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }
    }

    public static class Polling {
        private Duration interval = Duration.ofSeconds(30);
        private Duration runRetention = Duration.ofHours(2);
        private Duration lookback = Duration.ofMinutes(10);

        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }
        public Duration getRunRetention() { return runRetention; }
        public void setRunRetention(Duration runRetention) { this.runRetention = runRetention; }
        public Duration getLookback() { return lookback; }
        public void setLookback(Duration lookback) { this.lookback = lookback; }
    }
}
