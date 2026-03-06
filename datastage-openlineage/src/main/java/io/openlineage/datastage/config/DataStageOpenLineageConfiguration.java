package io.openlineage.datastage.config;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.datastage.client.DataStageClient;
import io.openlineage.datastage.client.IamTokenManager;
import io.openlineage.datastage.event.DataStageEventEmitter;
import io.openlineage.datastage.flow.FlowParser;
import io.openlineage.datastage.polling.DataStagePoller;
import io.openlineage.datastage.polling.RunStateTracker;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.net.URI;

@Configuration
@EnableConfigurationProperties({DataStageProperties.class, OpenLineageEmitterProperties.class})
public class DataStageOpenLineageConfiguration {

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
        return new OpenLineage(URI.create("https://github.com/OpenLineage/datastage-openlineage"));
    }

    @Bean
    public IamTokenManager iamTokenManager(DataStageProperties properties, RestClient.Builder restClientBuilder) {
        return new IamTokenManager(properties.getApiKey(), restClientBuilder);
    }

    @Bean
    public DataStageClient dataStageClient(DataStageProperties properties, IamTokenManager tokenManager,
                                           RestClient.Builder restClientBuilder) {
        return new DataStageClient(properties.getServiceUrl(), properties.getProjectId(),
                tokenManager, restClientBuilder);
    }

    @Bean
    public FlowParser flowParser(DataStageClient dataStageClient) {
        return new FlowParser(dataStageClient);
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
    public DataStagePoller dataStagePoller(DataStageClient client, FlowParser flowParser,
                                           RunStateTracker tracker, DataStageEventEmitter emitter,
                                           DataStageProperties properties) {
        return new DataStagePoller(client, flowParser, tracker, emitter, properties);
    }
}
