package io.openlineage.batch.extractor;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public class FallbackDatasetExtractor implements DatasetExtractor {

    @Override
    public boolean supports(Object component) {
        return true;
    }

    @Override
    public DatasetInfo extract(Object component) {
        DatasetInfo.Type type = (component instanceof ItemWriter<?>)
                ? DatasetInfo.Type.OUTPUT : DatasetInfo.Type.INPUT;
        return new DatasetInfo("unknown", component.getClass().getSimpleName(), type);
    }

    @Override
    public int getOrder() {
        return Integer.MAX_VALUE;
    }
}
