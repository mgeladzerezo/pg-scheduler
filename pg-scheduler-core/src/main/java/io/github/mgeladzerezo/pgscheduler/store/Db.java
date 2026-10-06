package io.github.mgeladzerezo.pgscheduler.store;

import io.github.mgeladzerezo.pgscheduler.SchedulerException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DataSourceUtils;

/**
 * The few lines of plain JDBC plumbing shared by the stores: three ways to obtain a connection, parameter
 * binding and row reading.
 *
 * <p>The three connection modes are the point of this class. Enqueue and admin operations
 * {@linkplain #joining join} whatever transaction the caller has open. Queue-internal operations (claim,
 * complete, reap) always use {@linkplain #ownTransaction their own} short transaction or a single
 * {@linkplain #autoCommit auto-commit} statement, because their correctness depends on committing
 * immediately and independently of any application transaction.
 */
public final class Db {

    /** SQLSTATE of a unique-constraint violation. */
    public static final String UNIQUE_VIOLATION = "23505";

    private final DataSource dataSource;

    public Db(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    /** Work that needs a connection. */
    @FunctionalInterface
    public interface SqlWork<T> {
        T run(Connection connection) throws SQLException;
    }

    /** Maps the current row of a result set. */
    @FunctionalInterface
    public interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /**
     * Runs on the connection Spring has bound to this thread if a transaction is active (so the work
     * commits or rolls back with the caller), otherwise on a fresh auto-commit connection.
     */
    public <T> T joining(SqlWork<T> work) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            return work.run(connection);
        } catch (SQLException e) {
            throw new SchedulerException("Database error: " + e.getMessage(), e);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    /** Runs in a dedicated transaction on a dedicated connection; never joins a caller's transaction. */
    public <T> T ownTransaction(SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(connection);
                throw e;
            } finally {
                connection.setAutoCommit(autoCommit);
            }
        } catch (SQLException e) {
            throw new SchedulerException("Database error: " + e.getMessage(), e);
        }
    }

    /** Runs on a dedicated auto-commit connection; never joins a caller's transaction. */
    public <T> T autoCommit(SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            return work.run(connection);
        } catch (SQLException e) {
            throw new SchedulerException("Database error: " + e.getMessage(), e);
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // The original failure is the one worth reporting.
        }
    }

    // ---- statement helpers ------------------------------------------------------------------------------

    public static int update(Connection connection, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bind(connection, ps, params);
            return ps.executeUpdate();
        }
    }

    public static <T> List<T> query(Connection connection, String sql, RowMapper<T> mapper, Object... params)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bind(connection, ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mapper.map(rs));
                }
                return rows;
            }
        }
    }

    /** The single row of a query, or {@code null} when it returns none. */
    public static <T> T queryOne(Connection connection, String sql, RowMapper<T> mapper, Object... params)
            throws SQLException {
        List<T> rows = query(connection, sql, mapper, params);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    /**
     * Binds positional parameters. {@link Instant} becomes a {@code timestamptz}; {@code String[]},
     * {@code Long[]} and {@code Integer[]} become PostgreSQL arrays; everything else goes through
     * {@code setObject}. JSON is passed as a string and cast with {@code ?::jsonb} in the SQL.
     */
    static void bind(Connection connection, PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object value = params[i];
            int index = i + 1;
            switch (value) {
                case null -> ps.setObject(index, null);
                case Instant instant -> ps.setObject(index, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));
                case String[] strings -> ps.setArray(index, connection.createArrayOf("text", strings));
                case Long[] longs -> ps.setArray(index, connection.createArrayOf("bigint", longs));
                case Integer[] ints -> ps.setArray(index, connection.createArrayOf("integer", ints));
                default -> ps.setObject(index, value);
            }
        }
    }

    public static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    public static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    public static Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }
}
