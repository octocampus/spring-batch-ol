package io.openlineage.batch.extractor;

import java.util.Comparator;
import java.util.List;

public class CompositeDatasetExtractor {

    private final List<DatasetExtractor> extractors;

    public CompositeDatasetExtractor(List<DatasetExtractor> extractors) {
        this.extractors = extractors.stream()
                .sorted(Comparator.comparingInt(DatasetExtractor::getOrder))
                .toList();
    }

    public DatasetInfo extract(Object component) {
        for (DatasetExtractor extractor : extractors) {
            if (extractor.supports(component)) {
                DatasetInfo info = extractor.extract(component);
                if (!info.isUnknown()) {
                    return info;
                }
            }
        }
        // All extractors returned UNKNOWN — return fallback
        DatasetInfo.Type type = (component instanceof org.springframework.batch.item.ItemWriter<?>)
                ? DatasetInfo.Type.OUTPUT : DatasetInfo.Type.INPUT;
        return DatasetInfo.unknown(type);
    }
}
