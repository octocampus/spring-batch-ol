package io.openlineage.datastage.dsjob.config;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.datastage.dsjob.command.DsjobRunner;
import io.openlineage.datastage.dsjob.dsx.DsxParser;
import io.openlineage.datastage.dsjob.event.DataStageEventEmitter;
import io.openlineage.datastage.dsjob.polling.DsjobPoller;
import io.openlineage.datastage.dsjob.polling.RunStateTracker;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
@EnableConfigurationProperties({DsjobProperties.class, OpenLineageEmitterProperties.class})
public class DsjobOpenLineageConfiguration {

    @Bean
    public OpenLineageClient openLineageClient(OpenLineageEmitterProperties properties) {
        return switch (properties.getTransport()) {
            case HTTP -> {
                var httpBuilder = HttpTransport.builder()
                        .uri(URI.create(properties.getHttp().getUrl()));
                if (properties.getHttp().getApiKey() != null) {
                    httpBuilder.apiKey(properties.getHttp().getApiKey());
                }
                yield OpenLineageClient.builder()
                        .transport(httpBuilder.build())
                        .build();
            }
            case CONSOLE -> OpenLineageClient.builder()
                    .transport(new ConsoleTransport())
                    .build();
            case NOOP -> OpenLineageClient.builder()
                    .transport(new NoopTransport())
                    .build();
        };
    }

    @Bean
    public OpenLineage openLineage() {
        return new OpenLineage(URI.create("https://github.com/OpenLineage/datastage-dsjob-openlineage"));
    }

    @Bean
    public DsjobRunner dsjobRunner(DsjobProperties properties) {
        return new DsjobRunner(properties.getDsHome());
    }

    @Bean
    public RunStateTracker runStateTracker() {
        return new RunStateTracker();
    }

    @Bean
    public DataStageEventEmitter dataStageEventEmitter(OpenLineageClient client, OpenLineage openLineage,
                                                       OpenLineageEmitterProperties properties) {
        return new DataStageEventEmitter(client, openLineage, properties);
    }

    @Bean
    public DsxParser dsxParser() {
        return new DsxParser();
    }

    @Bean
    public DsjobPoller dsjobPoller(DsjobRunner runner, RunStateTracker tracker,
                                   DataStageEventEmitter emitter, DsjobProperties properties,
                                   DsxParser dsxParser) {
        return new DsjobPoller(runner, tracker, emitter, properties, dsxParser);
    }
}
