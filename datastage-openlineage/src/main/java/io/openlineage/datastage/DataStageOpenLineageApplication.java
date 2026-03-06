package io.openlineage.datastage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataStageOpenLineageApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataStageOpenLineageApplication.class, args);
    }
}
