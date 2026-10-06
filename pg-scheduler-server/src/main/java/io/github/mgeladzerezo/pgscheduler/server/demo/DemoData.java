package io.github.mgeladzerezo.pgscheduler.server.demo;

import io.github.mgeladzerezo.pgscheduler.JobRequest;
import io.github.mgeladzerezo.pgscheduler.JobScheduler;
import io.github.mgeladzerezo.pgscheduler.cron.CronScheduler;
import io.github.mgeladzerezo.pgscheduler.cron.ScheduleDefinition;
import io.github.mgeladzerezo.pgscheduler.server.demo.DemoHandlers.Email;
import io.github.mgeladzerezo.pgscheduler.server.demo.DemoHandlers.Slow;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.ZoneId;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.stereotype.Component;

/**
 * Seeds schedules and, optionally, a steady trickle of jobs so that a fresh {@code docker compose up}
 * shows a living system. Only the dashboard container turns this on.
 */
@Component
public class DemoData implements ApplicationRunner {

    /**
     * @param seed    create the demo schedules (idempotent, by name) and a first batch of jobs
     * @param traffic seconds between randomly chosen demo jobs; 0 disables the trickle
     */
    @ConfigurationProperties("dashboard.demo")
    public record Settings(boolean seed, @DefaultValue("0") int traffic) {
    }

    private static final Logger log = LoggerFactory.getLogger(DemoData.class);

    private final Settings settings;
    private final CronScheduler cron;
    private final JobScheduler scheduler;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().daemon().name("demo-traffic").unstarted(r));

    public DemoData(Settings settings, CronScheduler cron, JobScheduler scheduler) {
        this.settings = settings;
        this.cron = cron;
        this.scheduler = scheduler;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (settings.seed()) {
            seedSchedules();
            for (int i = 0; i < 12; i++) {
                scheduler.enqueue(randomJob());
            }
        }
        if (settings.traffic() > 0) {
            executor.scheduleWithFixedDelay(this::trickle, settings.traffic(), settings.traffic(), TimeUnit.SECONDS);
        }
    }

    private void seedSchedules() {
        Email digest = new Email("team@example.com", "Minute digest");
        int created = 0;
        created += add(ScheduleDefinition.of("email-every-minute", "* * * * *", "email.send").withPayload(digest));
        created += add(ScheduleDefinition.of("flaky-every-2-minutes", "*/2 * * * *", "flaky.job"));
        created += add(ScheduleDefinition.of("slow-every-5-minutes", "*/5 * * * *", "slow.job")
                .withPayload(new Slow(15)));
        created += add(ScheduleDefinition.of("always-fails-every-10-minutes", "*/10 * * * *", "always.fail"));
        created += add(ScheduleDefinition.of("berlin-morning-report", "30 7 * * 1-5", "email.send")
                .withZone(ZoneId.of("Europe/Berlin")).withPayload(new Email("ops@example.com", "Morning report")));
        log.info("Demo schedules ready ({} newly created)", created);
    }

    private int add(ScheduleDefinition definition) {
        return cron.createIfAbsent(definition) ? 1 : 0;
    }

    private void trickle() {
        try {
            scheduler.enqueue(randomJob());
        } catch (RuntimeException e) {
            log.warn("Demo traffic could not enqueue: {}", e.toString());
        }
    }

    /** 65% email, 15% flaky, 10% slow, 10% always failing. */
    private static JobRequest randomJob() {
        int roll = ThreadLocalRandom.current().nextInt(100);
        if (roll < 65) {
            return JobRequest.of("email.send", new Email("user" + roll + "@example.com", "Welcome"));
        } else if (roll < 80) {
            return JobRequest.of("flaky.job");
        } else if (roll < 90) {
            return JobRequest.of("slow.job", new Slow(10 + roll % 10)).queue("default");
        }
        return JobRequest.of("always.fail").delay(Duration.ofSeconds(roll % 5));
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }
}
