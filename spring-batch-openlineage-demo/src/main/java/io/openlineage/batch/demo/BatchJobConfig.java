package io.openlineage.batch.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchJobConfig {

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personReader")
                .resource(new ClassPathResource("data/people.csv"))
                .delimited()
                .names("firstName", "lastName", "email", "age")
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Person(
                        fieldSet.readString("firstName"),
                        fieldSet.readString("lastName"),
                        fieldSet.readString("email"),
                        fieldSet.readInt("age")))
                .build();
    }

    @Bean
    public PersonProcessor processor() {
        return new PersonProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .sql("INSERT INTO people (first_name, last_name, email, age) VALUES (:firstName, :lastName, :email, :age)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean
    public Step csvToDatabaseStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   FlatFileItemReader<Person> reader,
                                   PersonProcessor processor,
                                   JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("csvToDatabaseStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job importPeopleJob(JobRepository jobRepository, Step csvToDatabaseStep) {
        return new JobBuilder("importPeopleJob", jobRepository)
                .start(csvToDatabaseStep)
                .build();
    }
}
