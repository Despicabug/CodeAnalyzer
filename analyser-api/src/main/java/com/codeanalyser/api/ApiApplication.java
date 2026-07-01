package com.codeanalyser.api;

import com.codeanalyser.worker.config.IngestionProperties;
import com.codeanalyser.worker.llm.LlmProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Application entry point. Scans all three modules (api, worker, common) in a
 * single Spring context so the pipeline starts alongside the REST API.
 */
@SpringBootApplication(scanBasePackages = "com.codeanalyser")
@EnableConfigurationProperties({IngestionProperties.class, LlmProperties.class})
public class ApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
