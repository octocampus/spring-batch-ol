package io.openlineage.batch.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;

@Configuration
public class AnalyticsJobConfig {

    @Bean
    public InitializingBean createOutputDir() {
        return () -> Files.createDirectories(Paths.get("output"));
    }

    // ── Step 1: enrichStep ──────────────────────────────────────────────

    @Bean
    public JdbcCursorItemReader<Person> enrichReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Person>()
                .name("enrichReader")
                .dataSource(dataSource)
                .sql("SELECT first_name, last_name, email, age FROM people")
                .rowMapper((rs, rowNum) -> new Person(
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("email"),
                        rs.getInt("age")))
                .build();
    }

    @Bean
    public PersonEnrichProcessor enrichProcessor() {
        return new PersonEnrichProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<EnrichedPerson> enrichWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<EnrichedPerson>()
                .sql("INSERT INTO enriched_people (first_name, last_name, email, age, full_name, category) "
                        + "VALUES (:firstName, :lastName, :email, :age, :fullName, :category)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean
    public Step enrichStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           JdbcCursorItemReader<Person> enrichReader,
                           PersonEnrichProcessor enrichProcessor,
                           JdbcBatchItemWriter<EnrichedPerson> enrichWriter) {
        return new StepBuilder("enrichStep", jobRepository)
                .<Person, EnrichedPerson>chunk(5, transactionManager)
                .reader(enrichReader)
                .processor(enrichProcessor)
                .writer(enrichWriter)
                .build();
    }

    // ── Step 2: exportSeniorsStep ───────────────────────────────────────

    @Bean
    public JdbcCursorItemReader<EnrichedPerson> seniorsReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<EnrichedPerson>()
                .name("seniorsReader")
                .dataSource(dataSource)
                .sql("SELECT first_name, last_name, email, age, full_name, category FROM enriched_people WHERE category = 'senior'")
                .rowMapper((rs, rowNum) -> new EnrichedPerson(
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("email"),
                        rs.getInt("age"),
                        rs.getString("full_name"),
                        rs.getString("category")))
                .build();
    }

    @Bean
    public FlatFileItemWriter<EnrichedPerson> seniorsWriter() {
        return new FlatFileItemWriterBuilder<EnrichedPerson>()
                .name("seniorsWriter")
                .resource(new FileSystemResource("output/seniors.csv"))
                .headerCallback(writer -> writer.write("firstName,lastName,email,age,fullName,category"))
                .delimited()
                .names("firstName", "lastName", "email", "age", "fullName", "category")
                .build();
    }

    @Bean
    public Step exportSeniorsStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  JdbcCursorItemReader<EnrichedPerson> seniorsReader,
                                  FlatFileItemWriter<EnrichedPerson> seniorsWriter) {
        return new StepBuilder("exportSeniorsStep", jobRepository)
                .<EnrichedPerson, EnrichedPerson>chunk(15, transactionManager)
                .reader(seniorsReader)
                .writer(seniorsWriter)
                .build();
    }

    // ── Step 3: exportJuniorsStep ───────────────────────────────────────

    @Bean
    public JdbcCursorItemReader<EnrichedPerson> juniorsReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<EnrichedPerson>()
                .name("juniorsReader")
                .dataSource(dataSource)
                .sql("SELECT first_name, last_name, email, age, full_name, category FROM enriched_people WHERE category = 'junior'")
                .rowMapper((rs, rowNum) -> new EnrichedPerson(
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("email"),
                        rs.getInt("age"),
                        rs.getString("full_name"),
                        rs.getString("category")))
                .build();
    }

    @Bean
    public FlatFileItemWriter<EnrichedPerson> juniorsWriter() {
        return new FlatFileItemWriterBuilder<EnrichedPerson>()
                .name("juniorsWriter")
                .resource(new FileSystemResource("output/juniors.csv"))
                .headerCallback(writer -> writer.write("firstName,lastName,email,age,fullName,category"))
                .delimited()
                .names("firstName", "lastName", "email", "age", "fullName", "category")
                .build();
    }

    @Bean
    public Step exportJuniorsStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  JdbcCursorItemReader<EnrichedPerson> juniorsReader,
                                  FlatFileItemWriter<EnrichedPerson> juniorsWriter) {
        return new StepBuilder("exportJuniorsStep", jobRepository)
                .<EnrichedPerson, EnrichedPerson>chunk(15, transactionManager)
                .reader(juniorsReader)
                .writer(juniorsWriter)
                .build();
    }

    // ── Step 4: statsStep ───────────────────────────────────────────────

    @Bean
    public StatsTasklet statsTasklet(JdbcTemplate jdbcTemplate) {
        return new StatsTasklet(jdbcTemplate);
    }

    @Bean
    public Step statsStep(JobRepository jobRepository,
                          PlatformTransactionManager transactionManager,
                          StatsTasklet statsTasklet) {
        return new StepBuilder("statsStep", jobRepository)
                .tasklet(statsTasklet, transactionManager)
                .build();
    }

    // ── Job ─────────────────────────────────────────────────────────────

    @Bean
    public Job analyticsJob(JobRepository jobRepository,
                            Step enrichStep,
                            Step exportSeniorsStep,
                            Step exportJuniorsStep,
                            Step statsStep) {
        return new JobBuilder("analyticsJob", jobRepository)
                .start(enrichStep)
                .next(exportSeniorsStep)
                .next(exportJuniorsStep)
                .next(statsStep)
                .build();
    }
}
