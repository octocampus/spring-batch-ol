package io.openlineage.batch.extractor;

public interface DatasetExtractor {

    boolean supports(Object component);

    DatasetInfo extract(Object component);

    int getOrder();
}
