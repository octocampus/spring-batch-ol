package io.openlineage.batch.config;

import io.openlineage.batch.event.OpenLineageEventEmitter;
import io.openlineage.batch.event.RunContext;
import io.openlineage.batch.extractor.CompositeDatasetExtractor;
import io.openlineage.batch.extractor.DatasetExtractor;
import io.openlineage.batch.extractor.FallbackDatasetExtractor;
import io.openlineage.batch.extractor.FlatFileDatasetExtractor;
import io.openlineage.batch.extractor.JdbcDatasetExtractor;
import io.openlineage.batch.listener.OpenLineageBatchConfigurer;
import io.openlineage.batch.listener.OpenLineageChunkListener;
import io.openlineage.batch.listener.OpenLineageJobListener;
import io.openlineage.batch.listener.OpenLineageStepListener;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import org.springframework.batch.core.Job;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.net.URI;
import java.util.List;

@AutoConfiguration
@ConditionalOnClass({Job.class, OpenLineageClient.class})
@ConditionalOnProperty(prefix = "openlineage", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(OpenLineageProperties.class)
public class OpenLineageAutoConfiguration {

    @Bean
    public OpenLineageClient openLineageClient(OpenLineageProperties properties) {
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
        return new OpenLineage(URI.create("https://github.com/OpenLineage/spring-batch-openlineage"));
    }

    @Bean
    public JdbcDatasetExtractor jdbcDatasetExtractor() {
        return new JdbcDatasetExtractor();
    }

    @Bean
    public FlatFileDatasetExtractor flatFileDatasetExtractor() {
        return new FlatFileDatasetExtractor();
    }

    @Bean
    public FallbackDatasetExtractor fallbackDatasetExtractor() {
        return new FallbackDatasetExtractor();
    }

    @Bean
    public CompositeDatasetExtractor compositeDatasetExtractor(List<DatasetExtractor> extractors) {
        return new CompositeDatasetExtractor(extractors);
    }

    @Bean
    public RunContext runContext() {
        return new RunContext();
    }

    @Bean
    public OpenLineageEventEmitter openLineageEventEmitter(
            OpenLineageClient client,
            OpenLineage openLineage,
            OpenLineageProperties properties,
            RunContext runContext) {
        return new OpenLineageEventEmitter(client, openLineage, properties, runContext);
    }

    @Bean
    public OpenLineageJobListener openLineageJobListener(
            OpenLineageEventEmitter emitter,
            OpenLineageProperties properties) {
        return new OpenLineageJobListener(emitter, properties);
    }

    @Bean
    public OpenLineageStepListener openLineageStepListener(
            OpenLineageEventEmitter emitter,
            CompositeDatasetExtractor extractor,
            OpenLineageProperties properties) {
        return new OpenLineageStepListener(emitter, extractor, properties);
    }

    @Bean
    public OpenLineageChunkListener openLineageChunkListener(
            OpenLineageEventEmitter emitter,
            OpenLineageProperties properties) {
        return new OpenLineageChunkListener(emitter, properties);
    }

    @Bean
    public OpenLineageBatchConfigurer openLineageBatchConfigurer(
            OpenLineageJobListener jobListener,
            OpenLineageStepListener stepListener,
            OpenLineageChunkListener chunkListener,
            CompositeDatasetExtractor extractor) {
        return new OpenLineageBatchConfigurer(jobListener, stepListener, chunkListener, extractor);
    }
}
