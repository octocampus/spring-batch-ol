package io.openlineage.batch.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public ApplicationRunner jobRunner(JobLauncher jobLauncher,
                                       Job importPeopleJob,
                                       Job analyticsJob) {
        return args -> {
            jobLauncher.run(importPeopleJob, new JobParameters());
            jobLauncher.run(analyticsJob, new JobParameters());
        };
    }
}
