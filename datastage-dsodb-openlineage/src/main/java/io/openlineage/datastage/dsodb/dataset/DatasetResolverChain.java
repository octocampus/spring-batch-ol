package io.openlineage.datastage.dsodb.dataset;

import io.openlineage.datastage.dsodb.config.DsodbProperties;
import io.openlineage.datastage.dsodb.dsx.DatasetDescriptor;
import io.openlineage.datastage.dsodb.dsx.DsxParser;
import io.openlineage.datastage.dsodb.xmeta.XmetaDatasetResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class DatasetResolverChain {

    private static final Logger log = LoggerFactory.getLogger(DatasetResolverChain.class);

    private final DsodbProperties properties;
    private final DsxParser dsxParser;
    private final XmetaDatasetResolver xmetaResolver;

    private volatile Map<String, List<DatasetDescriptor>> dsxDatasets;

    public DatasetResolverChain(DsodbProperties properties, DsxParser dsxParser,
                                XmetaDatasetResolver xmetaResolver) {
        this.properties = properties;
        this.dsxParser = dsxParser;
        this.xmetaResolver = xmetaResolver;
        this.dsxDatasets = loadDsxDatasets();
    }

    public List<DatasetDescriptor> resolve(String projectName, String jobName) {
        // 1. Try .dsx files first (local, fast)
        List<DatasetDescriptor> fromDsx = dsxDatasets.get(jobName);
        if (fromDsx != null && !fromDsx.isEmpty()) {
            return fromDsx;
        }

        // 2. Try xmeta SQL (if enabled)
        if (xmetaResolver != null) {
            try {
                List<DatasetDescriptor> fromXmeta = xmetaResolver.resolveDatasets(projectName, jobName);
                if (!fromXmeta.isEmpty()) {
                    return fromXmeta;
                }
            } catch (Exception e) {
                log.warn("xmeta resolution failed for {}/{}", projectName, jobName, e);
            }
        }

        return List.of();
    }

    public void reloadDsx() {
        this.dsxDatasets = loadDsxDatasets();
    }

    private Map<String, List<DatasetDescriptor>> loadDsxDatasets() {
        String dsxDir = properties.getDsxDirectory();
        if (dsxDir == null || dsxDir.isBlank()) {
            log.info("No DSX directory configured");
            return Map.of();
        }
        return dsxParser.parseDirectory(Path.of(dsxDir));
    }
}
