package dev.example.raftkv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RaftKvApplication {
    public static void main(String[] args) {
        SpringApplication.run(RaftKvApplication.class, args);
    }
}
