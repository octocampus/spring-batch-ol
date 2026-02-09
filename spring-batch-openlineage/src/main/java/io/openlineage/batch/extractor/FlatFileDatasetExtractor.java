package io.openlineage.batch.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import java.lang.reflect.Field;

public class FlatFileDatasetExtractor implements DatasetExtractor {

    private static final Logger log = LoggerFactory.getLogger(FlatFileDatasetExtractor.class);

    @Override
    public boolean supports(Object component) {
        return component instanceof FlatFileItemReader<?>
                || component instanceof FlatFileItemWriter<?>;
    }

    @Override
    public DatasetInfo extract(Object component) {
        try {
            if (component instanceof FlatFileItemReader<?> reader) {
                return extractFromReader(reader);
            } else if (component instanceof FlatFileItemWriter<?> writer) {
                return extractFromWriter(writer);
            }
        } catch (Exception e) {
            log.warn("Failed to extract FlatFile dataset info from {}", component.getClass().getSimpleName(), e);
        }
        return DatasetInfo.unknown(component instanceof FlatFileItemReader<?>
                ? DatasetInfo.Type.INPUT : DatasetInfo.Type.OUTPUT);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private DatasetInfo extractFromReader(FlatFileItemReader<?> reader) {
        Resource resource = getField(reader, "resource", Resource.class);
        return buildDatasetInfo(resource, DatasetInfo.Type.INPUT);
    }

    private DatasetInfo extractFromWriter(FlatFileItemWriter<?> writer) {
        Resource resource = getField(writer, "resource", Resource.class);
        return buildDatasetInfo(resource, DatasetInfo.Type.OUTPUT);
    }

    private DatasetInfo buildDatasetInfo(Resource resource, DatasetInfo.Type type) {
        if (resource == null) {
            return DatasetInfo.unknown(type);
        }

        String namespace;
        String name;

        if (resource instanceof ClassPathResource cpr) {
            namespace = "file:classpath";
            name = cpr.getPath();
        } else if (resource instanceof FileSystemResource fsr) {
            namespace = "file:" + fsr.getFile().getParent();
            name = fsr.getFilename();
        } else if (resource instanceof UrlResource) {
            try {
                namespace = resource.getURL().getProtocol() + "://" + resource.getURL().getHost();
                name = resource.getURL().getPath();
            } catch (Exception e) {
                namespace = "file:unknown";
                name = resource.getDescription();
            }
        } else {
            namespace = "file:unknown";
            name = resource.getDescription();
        }

        return new DatasetInfo(namespace, name, type);
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName, Class<T> type) {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
