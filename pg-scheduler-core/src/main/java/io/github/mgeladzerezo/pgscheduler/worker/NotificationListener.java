package io.github.mgeladzerezo.pgscheduler.worker;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds one connection in {@code LISTEN} mode and fans notifications out to in-process subscribers.
 *
 * <p>This is what lets workers sleep for a whole poll interval and still start a job milliseconds after it
 * is enqueued: the enqueue transaction issues {@code NOTIFY}, PostgreSQL delivers it at commit, and the
 * subscriber wakes its poller.
 *
 * <p>Notifications are a hint, never a guarantee. They are lost while the connection is down, so after
 * every (re)connect each subscriber is called once with a {@code null} payload meaning "check now", and
 * the pollers keep their interval as a safety net.
 */
public final class NotificationListener {

    /** Opens the connection to listen on. Should not come from a small pool: it is held for good. */
    @FunctionalInterface
    public interface ConnectionSource {
        Connection open() throws SQLException;
    }

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);
    /** Upper bound of one blocking read; also how quickly {@link #stop} is noticed. */
    private static final int READ_TIMEOUT_MS = 500;
    /** Send a trivial query this often to find out about connections that died silently. */
    private static final long PROBE_INTERVAL_NANOS = 15_000_000_000L;

    private final ConnectionSource connectionSource;
    private final Map<String, List<Consumer<String>>> subscribers = new ConcurrentHashMap<>();
    private volatile boolean running;
    private volatile boolean connected;
    private Thread thread;

    public NotificationListener(ConnectionSource connectionSource) {
        this.connectionSource = connectionSource;
    }

    /**
     * Subscribes to a channel. Must be called before {@link #start}. The callback receives the notification
     * payload, or {@code null} after a (re)connect; it runs on the listener thread and must not block.
     */
    public void subscribe(String channel, Consumer<String> callback) {
        if (!channel.matches("[a-z_][a-z0-9_]*")) {
            throw new IllegalArgumentException("invalid channel name: " + channel);
        }
        subscribers.computeIfAbsent(channel, c -> new CopyOnWriteArrayList<>()).add(callback);
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().daemon().name("pgs-listener").start(this::run);
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    /** Whether the LISTEN connection is currently established. */
    public boolean isConnected() {
        return connected;
    }

    private void run() {
        long backoffMs = 250;
        while (running) {
            try (Connection connection = connectionSource.open()) {
                connection.setAutoCommit(true);
                try (Statement statement = connection.createStatement()) {
                    for (String channel : subscribers.keySet()) {
                        statement.execute("LISTEN " + channel);
                    }
                }
                PGConnection pg = connection.unwrap(PGConnection.class);
                connected = true;
                backoffMs = 250;
                log.debug("Listening on {}", subscribers.keySet());
                subscribers.values().forEach(callbacks -> callbacks.forEach(callback -> deliver(callback, null)));

                long lastProbe = System.nanoTime();
                while (running) {
                    PGNotification[] notifications = pg.getNotifications(READ_TIMEOUT_MS);
                    if (notifications != null) {
                        for (PGNotification notification : notifications) {
                            List<Consumer<String>> callbacks = subscribers.get(notification.getName());
                            if (callbacks != null) {
                                callbacks.forEach(callback -> deliver(callback, notification.getParameter()));
                            }
                        }
                    }
                    if (System.nanoTime() - lastProbe > PROBE_INTERVAL_NANOS) {
                        try (Statement probe = connection.createStatement()) {
                            probe.execute("SELECT 1");
                        }
                        lastProbe = System.nanoTime();
                    }
                }
                // A pooled connection goes back to its pool: do not leave it subscribed.
                try (Statement statement = connection.createStatement()) {
                    statement.execute("UNLISTEN *");
                }
            } catch (SQLException | RuntimeException e) {
                if (running) {
                    log.warn("LISTEN connection lost ({}); polling continues, reconnecting in {} ms", e.toString(), backoffMs);
                    sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 10_000);
                }
            } finally {
                connected = false;
            }
        }
    }

    private static void deliver(Consumer<String> callback, String payload) {
        try {
            callback.accept(payload);
        } catch (RuntimeException e) {
            log.warn("Notification subscriber failed", e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
