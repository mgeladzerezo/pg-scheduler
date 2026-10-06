package io.github.mgeladzerezo.pgscheduler.testsupport;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.mgeladzerezo.pgscheduler.store.Db;
import io.github.mgeladzerezo.pgscheduler.store.SchemaMigrator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A fresh, migrated database inside one PostgreSQL 16 container shared by the whole test JVM.
 *
 * <p>Each test class gets its own database, so classes cannot see each other's jobs, and every pool handed
 * out is closed with it. Pools are real and separate ({@link #newPool}): tests that claim to show several
 * workers racing do so over separate connections, as separate processes would.
 */
public final class TestDatabase implements AutoCloseable {

    // Default durability settings on purpose: throughput measured in tests should not be flattered by fsync=off.
    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "max_connections=300");
    private static final AtomicInteger COUNTER = new AtomicInteger();

    static {
        CONTAINER.start();
    }

    private final String name;
    private final List<HikariDataSource> pools = new ArrayList<>();
    private final HikariDataSource shared;

    private TestDatabase(String name) {
        this.name = name;
        this.shared = newPool(4);
    }

    /** Creates a new database with the scheduler schema applied. */
    public static TestDatabase create() {
        String name = "t" + COUNTER.incrementAndGet() + "_" + Long.toString(System.nanoTime(), 36);
        try (Connection connection = DriverManager.getConnection(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(),
                CONTAINER.getPassword()); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        TestDatabase database = new TestDatabase(name);
        SchemaMigrator.migrate(database.shared);
        return database;
    }

    public String jdbcUrl() {
        return CONTAINER.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + name + "$1");
    }

    public String username() {
        return CONTAINER.getUsername();
    }

    public String password() {
        return CONTAINER.getPassword();
    }

    /** A new, independent connection pool on this database. Closed when the database is closed. */
    public synchronized HikariDataSource newPool(int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl());
        config.setUsername(username());
        config.setPassword(password());
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(20_000);
        config.setPoolName("test-" + name + "-" + pools.size());
        HikariDataSource pool = new HikariDataSource(config);
        pools.add(pool);
        return pool;
    }

    /** A small pool for assertions and set-up. */
    public HikariDataSource dataSource() {
        return shared;
    }

    /** A dedicated, unpooled connection, as the LISTEN connection would be in production. */
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), username(), password());
    }

    public long count(String sql, Object... params) {
        Long n = new Db(shared).autoCommit(c -> Db.queryOne(c, sql, rs -> rs.getLong(1), params));
        return n == null ? 0 : n;
    }

    public String string(String sql, Object... params) {
        return new Db(shared).autoCommit(c -> Db.queryOne(c, sql, rs -> rs.getString(1), params));
    }

    public <T> List<T> list(String sql, Db.RowMapper<T> mapper, Object... params) {
        return new Db(shared).autoCommit(c -> Db.query(c, sql, mapper, params));
    }

    public void execute(String sql, Object... params) {
        new Db(shared).autoCommit(c -> Db.update(c, sql, params));
    }

    /** State of a job as text, or {@code null} if it does not exist. */
    public String jobState(long jobId) {
        return string("SELECT state FROM pgs_job WHERE id = ?", jobId);
    }

    @Override
    public synchronized void close() {
        pools.forEach(HikariDataSource::close);
        pools.clear();
        try (Connection connection = DriverManager.getConnection(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(),
                CONTAINER.getPassword()); Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
        } catch (SQLException e) {
            // Leaving a database behind in a throw-away container is harmless.
        }
    }
}
