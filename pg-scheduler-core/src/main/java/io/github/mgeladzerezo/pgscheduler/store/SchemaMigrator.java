package io.github.mgeladzerezo.pgscheduler.store;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * Creates and upgrades the scheduler's tables with a private Flyway instance.
 *
 * <p>The migrations live in {@code classpath:db/pgscheduler} and are tracked in their own history table,
 * {@code pgs_schema_history}. The host application's Flyway (default location {@code db/migration},
 * default table {@code flyway_schema_history}) neither sees nor is affected by them, so the library can be
 * added to an application with an existing schema, and upgraded independently of it. Flyway takes a
 * PostgreSQL advisory lock while migrating, so several nodes starting at once is safe.
 */
public final class SchemaMigrator {

    private SchemaMigrator() {
    }

    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/pgscheduler")
                .table("pgs_schema_history")
                // The schema usually already contains the application's own tables.
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
    }
}
