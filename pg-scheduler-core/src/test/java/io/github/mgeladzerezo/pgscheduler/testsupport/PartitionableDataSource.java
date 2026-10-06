package io.github.mgeladzerezo.pgscheduler.testsupport;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * A data source that can be cut off from the database and reconnected, to stage a network partition that
 * later heals. While partitioned, every attempt to get a connection fails; the node behind it can neither
 * heartbeat nor report results, but its handler threads keep running, which is exactly the zombie-worker
 * situation fencing exists for.
 */
public final class PartitionableDataSource extends DelegatingDataSource {

    private volatile boolean partitioned;

    public PartitionableDataSource(DataSource target) {
        super(target);
    }

    public void partition() {
        partitioned = true;
    }

    public void heal() {
        partitioned = false;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (partitioned) {
            throw new SQLException("simulated network partition", "08006");
        }
        return super.getConnection();
    }
}
