package io.github.mgeladzerezo.pgscheduler.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The dashboard and demo application. The same jar runs in every role: with
 * {@code pgscheduler.worker.enabled=true} it is a worker, with {@code pgscheduler.cron.enabled=true} it
 * fires schedules, and it always serves the dashboard.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PgSchedulerServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PgSchedulerServerApplication.class, args);
    }
}
