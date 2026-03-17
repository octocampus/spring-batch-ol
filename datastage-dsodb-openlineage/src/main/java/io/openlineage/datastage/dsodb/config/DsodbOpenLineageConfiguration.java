package io.openlineage.datastage.dsodb.config;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.NoopTransport;
import io.openlineage.datastage.dsodb.dataset.DatasetResolverChain;
import io.openlineage.datastage.dsodb.dsx.DsxParser;
import io.openlineage.datastage.dsodb.event.DataStageEventEmitter;
import io.openlineage.datastage.dsodb.polling.DsodbPoller;
import io.openlineage.datastage.dsodb.polling.RunStateTracker;
import io.openlineage.datastage.dsodb.repository.DsodbJobRepository;
import io.openlineage.datastage.dsodb.xmeta.XmetaDatasetResolver;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.net.URI;
import java.util.Optional;

@Configuration
@EnableConfigurationProperties({DsodbProperties.class, OpenLineageEmitterProperties.class})
public class DsodbOpenLineageConfiguration {

    @Bean
    public DataSource dsodbDataSource(DsodbProperties props) {
        var dsodb = props.getDsodb();
        var ds = new DriverManagerDataSource();
        if (dsodb.getUrl() != null) {
            ds.setUrl(dsodb.getUrl());
            ds.setUsername(dsodb.getUsername());
            ds.setPassword(dsodb.getPassword());
            ds.setDriverClassName(dsodb.getDriverClassName());
        }
        return ds;
    }

    @Bean
    public NamedParameterJdbcTemplate dsodbJdbcTemplate(DataSource dsodbDataSource) {
        return new NamedParameterJdbcTemplate(dsodbDataSource);
    }

    @Bean
    @ConditionalOnProperty(prefix = "datastage.xmeta", name = "enabled", havingValue = "true")
    public DataSource xmetaDataSource(DsodbProperties props) {
        var xmeta = props.getXmeta();
        var ds = new DriverManagerDataSource();
        ds.setUrl(xmeta.getUrl());
        ds.setUsername(xmeta.getUsername());
        ds.setPassword(xmeta.getPassword());
        ds.setDriverClassName(xmeta.getDriverClassName());
        return ds;
    }

    @Bean
    @ConditionalOnProperty(prefix = "datastage.xmeta", name = "enabled", havingValue = "true")
    public XmetaDatasetResolver xmetaDatasetResolver(DsodbProperties props) {
        var xmeta = props.getXmeta();
        var ds = new DriverManagerDataSource();
        ds.setUrl(xmeta.getUrl());
        ds.setUsername(xmeta.getUsername());
        ds.setPassword(xmeta.getPassword());
        ds.setDriverClassName(xmeta.getDriverClassName());
        return new XmetaDatasetResolver(new JdbcTemplate(ds), xmeta.getSchema());
    }

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
        return new OpenLineage(URI.create("https://github.com/OpenLineage/datastage-dsodb-openlineage"));
    }

    @Bean
    public DsodbJobRepository dsodbJobRepository(NamedParameterJdbcTemplate dsodbJdbcTemplate,
                                                  DsodbProperties props) {
        return new DsodbJobRepository(dsodbJdbcTemplate, props.getDsodb().getSchema());
    }

    @Bean
    public DsxParser dsxParser() {
        return new DsxParser();
    }

    @Bean
    public DatasetResolverChain datasetResolverChain(DsodbProperties props, DsxParser dsxParser,
                                                      Optional<XmetaDatasetResolver> xmetaResolver) {
        return new DatasetResolverChain(props, dsxParser, xmetaResolver.orElse(null));
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
    public DsodbPoller dsodbPoller(DsodbJobRepository repository, RunStateTracker tracker,
                                   DataStageEventEmitter emitter, DsodbProperties properties,
                                   DatasetResolverChain datasetResolver) {
        return new DsodbPoller(repository, tracker, emitter, properties, datasetResolver);
    }
}
