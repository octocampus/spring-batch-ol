package io.openlineage.datastage.dsjob;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataStageDsjobApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataStageDsjobApplication.class, args);
    }
}
