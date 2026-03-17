package io.openlineage.datastage.dsodb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataStageDsodbApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataStageDsodbApplication.class, args);
    }
}
