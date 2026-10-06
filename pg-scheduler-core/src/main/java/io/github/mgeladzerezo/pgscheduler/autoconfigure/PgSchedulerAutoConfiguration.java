package io.github.mgeladzerezo.pgscheduler.autoconfigure;

import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.JobScheduler;
import io.github.mgeladzerezo.pgscheduler.SchedulerNode;
import io.github.mgeladzerezo.pgscheduler.admin.SchedulerAdmin;
import io.github.mgeladzerezo.pgscheduler.cron.CronScheduler;
import io.github.mgeladzerezo.pgscheduler.metrics.MicrometerSchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.metrics.SchedulerEvents;
import io.github.mgeladzerezo.pgscheduler.store.SchemaMigrator;
import io.github.mgeladzerezo.pgscheduler.worker.HandlerRegistry;
import io.github.mgeladzerezo.pgscheduler.worker.Maintenance;
import io.github.mgeladzerezo.pgscheduler.worker.NotificationListener;
import io.github.mgeladzerezo.pgscheduler.worker.WorkerOptions;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.DriverManager;
import java.time.Clock;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import tools.jackson.databind.ObjectMapper;

/**
 * Wires a {@link SchedulerNode} from {@code pgscheduler.*} properties and the application's
 * {@link DataSource}. Beans that applications use: {@link JobScheduler} (enqueue, joins the caller's
 * transaction), {@link SchedulerAdmin} and {@link CronScheduler} (schedule management).
 *
 * <p>The node is started when the context has finished creating its singletons, after
 * {@link JobHandlerRegistrar} has registered every handler, and stopped (gracefully) before the web server.
 */
@AutoConfiguration(afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "pgscheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PgSchedulerProperties.class)
public class PgSchedulerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    HandlerRegistry pgSchedulerHandlerRegistry() {
        return new HandlerRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    JobPolicies pgSchedulerJobPolicies(PgSchedulerProperties properties) {
        return new JobPolicies(JobHandlerRegistrar.override(JobPolicy.DEFAULT, properties.defaults()));
    }

    @Bean
    JobHandlerRegistrar pgSchedulerJobHandlerRegistrar(ConfigurableListableBeanFactory beanFactory,
                                                       HandlerRegistry registry, JobPolicies policies,
                                                       PgSchedulerProperties properties) {
        return new JobHandlerRegistrar(beanFactory, registry, policies, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    SchedulerNode pgSchedulerNode(DataSource dataSource, PgSchedulerProperties properties, HandlerRegistry handlers,
                                  JobPolicies policies, ObjectProvider<ObjectMapper> json, ObjectProvider<Clock> clock,
                                  ObjectProvider<SchedulerEvents> events, Environment environment) {
        if (properties.migrate()) {
            SchemaMigrator.migrate(dataSource);
        }
        SchedulerNode.Builder builder = SchedulerNode.builder(dataSource)
                .handlers(handlers)
                .policies(policies)
                .clock(clock.getIfAvailable(Clock::systemUTC))
                .events(events.getIfAvailable(() -> SchedulerEvents.NONE));
        json.ifAvailable(builder::json);
        if (properties.workerId() != null && !properties.workerId().isBlank()) {
            builder.workerId(properties.workerId());
        }
        PgSchedulerProperties.Worker w = properties.worker();
        if (w.enabled()) {
            builder.worker(new WorkerOptions(w.queues(), w.batchSize(), w.pollInterval(), w.lease(),
                    w.heartbeat() != null ? w.heartbeat() : w.lease().dividedBy(3), w.shutdownTimeout()));
        }
        PgSchedulerProperties.Maintenance m = properties.maintenance();
        if (m.enabled()) {
            builder.maintenance(new Maintenance.Options(m.interval(), m.reapInterval(),
                    Maintenance.Options.defaults().batchSize(), m.finishedRetention(),
                    Maintenance.Options.defaults().workerSilence()));
        }
        PgSchedulerProperties.Cron c = properties.cron();
        if (c.enabled()) {
            builder.cron(new CronScheduler.Options(c.misfireThreshold(), c.maxCatchUpPerTick(), c.pollInterval()));
        }
        if (properties.listen().enabled() && (w.enabled() || m.enabled())) {
            builder.listen(listenConnections(dataSource, environment));
        }
        return builder.build();
    }

    /**
     * The LISTEN connection is held for the life of the process, so it is opened outside the pool when the
     * connection properties are known; otherwise one pooled connection is borrowed for good.
     */
    private static NotificationListener.ConnectionSource listenConnections(DataSource dataSource, Environment environment) {
        String url = environment.getProperty("spring.datasource.url");
        if (url == null) {
            return dataSource::getConnection;
        }
        String user = environment.getProperty("spring.datasource.username");
        String password = environment.getProperty("spring.datasource.password");
        return () -> DriverManager.getConnection(url, user, password);
    }

    @Bean
    @ConditionalOnMissingBean
    JobScheduler pgSchedulerJobScheduler(SchedulerNode node) {
        return node.scheduler();
    }

    @Bean
    @ConditionalOnMissingBean
    SchedulerAdmin pgSchedulerAdmin(SchedulerNode node) {
        return node.admin();
    }

    @Bean
    @ConditionalOnMissingBean
    CronScheduler pgSchedulerCron(SchedulerNode node) {
        return node.cron();
    }

    @Bean
    SmartLifecycle pgSchedulerLifecycle(SchedulerNode node) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                node.start();
                running = true;
            }

            @Override
            public void stop() {
                node.close();
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return Integer.MAX_VALUE - 1000;
            }
        };
    }

    /** Meters, only when Micrometer and a registry are present. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class MetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean(SchedulerEvents.class)
        SchedulerEvents pgSchedulerEvents(ObjectProvider<MeterRegistry> registry) {
            MeterRegistry meterRegistry = registry.getIfAvailable();
            return meterRegistry == null ? SchedulerEvents.NONE : new MicrometerSchedulerEvents(meterRegistry);
        }

        @Bean
        QueueDepthMetrics pgSchedulerQueueDepthMetrics(SchedulerAdmin admin) {
            return new QueueDepthMetrics(admin, java.time.Duration.ofSeconds(5));
        }
    }
}
