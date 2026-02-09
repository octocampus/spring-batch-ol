package io.openlineage.batch.listener;

import io.openlineage.batch.extractor.CompositeDatasetExtractor;
import io.openlineage.batch.extractor.DatasetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class OpenLineageBatchConfigurer implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(OpenLineageBatchConfigurer.class);

    private final OpenLineageJobListener jobListener;
    private final OpenLineageStepListener stepListener;
    private final OpenLineageChunkListener chunkListener;
    private final CompositeDatasetExtractor extractor;

    public OpenLineageBatchConfigurer(
            OpenLineageJobListener jobListener,
            OpenLineageStepListener stepListener,
            OpenLineageChunkListener chunkListener,
            CompositeDatasetExtractor extractor) {
        this.jobListener = jobListener;
        this.stepListener = stepListener;
        this.chunkListener = chunkListener;
        this.extractor = extractor;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof AbstractJob job) {
            job.registerJobExecutionListener(jobListener);
            log.debug("Registered OpenLineage JobExecutionListener on job '{}'", beanName);
        }

        if (bean instanceof TaskletStep step) {
            step.registerStepExecutionListener(stepListener);
            step.registerChunkListener(chunkListener);
            log.debug("Registered OpenLineage Step/Chunk listeners on step '{}'", beanName);

            extractAndRegisterComponents(step);
        }

        return bean;
    }

    private void extractAndRegisterComponents(TaskletStep step) {
        try {
            Object tasklet = getField(step, "tasklet", Object.class);
            if (tasklet == null) return;

            // ChunkOrientedTasklet -> chunkProvider -> itemReader
            Object chunkProvider = getField(tasklet, "chunkProvider", Object.class);
            ItemReader<?> reader = null;
            if (chunkProvider != null) {
                reader = getField(chunkProvider, "itemReader", ItemReader.class);
            }

            // ChunkOrientedTasklet -> chunkProcessor -> itemWriter
            Object chunkProcessor = getField(tasklet, "chunkProcessor", Object.class);
            ItemWriter<?> writer = null;
            if (chunkProcessor != null) {
                writer = getField(chunkProcessor, "itemWriter", ItemWriter.class);
            }

            stepListener.registerStepComponents(step.getName(), reader, writer);

            // Resolve output datasets for chunk listener using the extractor
            if (writer != null) {
                List<DatasetInfo> outputs = new ArrayList<>();
                DatasetInfo info = extractor.extract(writer);
                outputs.add(info);
                chunkListener.registerStepOutputDatasets(step.getName(), outputs);
            }

            log.debug("Extracted components for step '{}': reader={}, writer={}",
                    step.getName(),
                    reader != null ? reader.getClass().getSimpleName() : "null",
                    writer != null ? writer.getClass().getSimpleName() : "null");
        } catch (Exception e) {
            log.warn("Failed to extract reader/writer from step '{}'", step.getName(), e);
        }
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
