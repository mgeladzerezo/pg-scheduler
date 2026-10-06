package io.github.mgeladzerezo.pgscheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.autoconfigure.PgSchedulerAutoConfiguration;
import io.github.mgeladzerezo.pgscheduler.testsupport.TestDatabase;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The Spring Boot integration: handlers found in the context, typed payloads, policy precedence. */
class AutoConfigurationTest {

    record Note(String text) {
    }

    record Line(String sku, int quantity) {
    }

    static final List<Object> SEEN = new CopyOnWriteArrayList<>();

    static class Handlers {
        @io.github.mgeladzerezo.pgscheduler.annotation.JobHandler(value = "note.add", maxAttempts = 2, timeout = "2s")
        String add(Note note, JobContext context) {
            SEEN.add(note);
            return "stored " + note.text() + " on attempt " + context.attempt();
        }

        @io.github.mgeladzerezo.pgscheduler.annotation.JobHandler("lines.sum")
        int sum(List<Line> lines) {
            SEEN.add(lines.getFirst());
            return lines.stream().mapToInt(Line::quantity).sum();
        }
    }

    static class Ping implements JobHandler<Note> {
        @Override
        public String jobType() {
            return "ping";
        }

        @Override
        public void handle(Note payload, JobContext context) {
            SEEN.add("ping:" + payload.text());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AppConfig {
        static DataSource dataSource;

        // shared by several contexts: the test class closes it, not the context
        @Bean(destroyMethod = "")
        DataSource dataSource() {
            return dataSource;
        }

        @Bean
        Handlers handlers() {
            return new Handlers();
        }

        @Bean
        Ping ping() {
            return new Ping();
        }
    }

    private static TestDatabase database;

    @BeforeAll
    static void setUp() {
        database = TestDatabase.create();
        AppConfig.dataSource = database.newPool(8);
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @Test
    void handlersAreDiscoveredAndPoliciesFollowTheDocumentedPrecedence() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgSchedulerAutoConfiguration.class))
                .withUserConfiguration(AppConfig.class)
                .withPropertyValues(
                        "pgscheduler.worker.poll-interval=100ms",
                        "pgscheduler.worker.queues.default=4",
                        "pgscheduler.defaults.max-attempts=9",
                        "pgscheduler.job-types[ping].max-attempts=3",
                        "pgscheduler.job-types[lines.sum].rate-limit-per-second=50")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    JobPolicies policies = context.getBean(JobPolicies.class);
                    // annotation beats pgscheduler.defaults; pgscheduler.job-types beats the annotation
                    assertThat(policies.forType("note.add").maxAttempts()).isEqualTo(2);
                    assertThat(policies.forType("note.add").timeout()).isEqualTo(Duration.ofSeconds(2));
                    assertThat(policies.forType("ping").maxAttempts()).isEqualTo(3);
                    assertThat(policies.forType("lines.sum").maxAttempts()).isEqualTo(9);
                    assertThat(policies.forType("lines.sum").rateLimit().permitsPerSecond()).isEqualTo(50);

                    JobScheduler scheduler = context.getBean(JobScheduler.class);
                    SchedulerAdmin admin = context.getBean(SchedulerAdmin.class);
                    long noteId = scheduler.enqueue("note.add", new Note("hello")).jobId();
                    long sumId = scheduler.enqueue("lines.sum", List.of(new Line("a", 2), new Line("b", 5))).jobId();
                    scheduler.enqueue("ping", new Note("pong"));

                    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                        assertThat(admin.job(noteId).state()).isEqualTo(JobState.SUCCEEDED);
                        assertThat(admin.job(sumId).state()).isEqualTo(JobState.SUCCEEDED);
                    });
                    assertThat(admin.job(noteId).resultJson()).contains("stored hello on attempt 1");
                    assertThat(admin.job(sumId).resultJson()).isEqualTo("7");
                    // the element type survived generics: a Line, not a Map
                    assertThat(SEEN).contains(new Note("hello"), new Line("a", 2), "ping:pong");
                });
    }

    @Test
    void disabledSwitchLeavesNothingBehind() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgSchedulerAutoConfiguration.class))
                .withUserConfiguration(AppConfig.class)
                .withPropertyValues("pgscheduler.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(JobScheduler.class));
    }

    @Test
    void aClientOnlyApplicationCanTurnEveryRoleOff() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgSchedulerAutoConfiguration.class))
                .withUserConfiguration(AppConfig.class)
                .withPropertyValues("pgscheduler.worker.enabled=false", "pgscheduler.maintenance.enabled=false")
                .run(context -> {
                    SchedulerNode node = context.getBean(SchedulerNode.class);
                    assertThat(node.worker()).isNull();
                    assertThat(node.maintenance()).isNull();
                    assertThat(node.listener()).isNull();
                    assertThat(context.getBean(JobScheduler.class).enqueue("note.add", Map.of("text", "x")).created())
                            .isTrue();
                });
    }
}
