package io.github.mgeladzerezo.pgscheduler.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** The whole server against a real PostgreSQL: authentication, the JSON API, the demo handlers and the stream. */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = {
        "dashboard.auth.password=secret",
        "pgscheduler.worker.poll-interval=200ms",
        "pgscheduler.cron.enabled=true",
        "spring.datasource.hikari.maximum-pool-size=12"})
class DashboardApiTest {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Value("${local.server.port}")
    int port;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper json = JsonMapper.builder().build();
    private static final String AUTH = "Basic " + Base64.getEncoder().encodeToString("admin:secret".getBytes(StandardCharsets.UTF_8));

    private HttpResponse<String> call(String method, String path, String body, boolean authenticated) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json");
        if (authenticated) {
            request.header("Authorization", AUTH);
        }
        request.method(method, body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body));
        return client.send(request.build(), BodyHandlers.ofString());
    }

    private JsonNode call(String method, String path, String body) throws Exception {
        HttpResponse<String> response = call(method, path, body, true);
        assertThat(response.statusCode()).as(method + " " + path + " -> " + response.body()).isLessThan(300);
        return response.body().isEmpty() ? null : json.readTree(response.body());
    }

    private JsonNode job(long id) throws Exception {
        return call("GET", "/api/jobs/" + id, null);
    }

    @Test
    void everythingButTheHealthProbeNeedsTheAdminLogin() throws Exception {
        assertThat(call("GET", "/api/overview", null, false).statusCode()).isEqualTo(401);
        assertThat(call("GET", "/", null, false).statusCode()).isEqualTo(401);
        assertThat(call("GET", "/actuator/prometheus", null, false).statusCode()).isEqualTo(401);
        assertThat(call("GET", "/actuator/health", null, false).statusCode()).isEqualTo(200);
        HttpRequest wrong = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/overview"))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString("admin:nope".getBytes())).build();
        assertThat(client.send(wrong, BodyHandlers.ofString()).statusCode()).isEqualTo(401);
        assertThat(call("GET", "/", null, true).body()).contains("pg-scheduler");
    }

    @Test
    void anEnqueuedEmailRunsAndShowsItsResultAndAttemptTimeline() throws Exception {
        long id = call("POST", "/api/jobs", """
                {"type":"email.send","payload":{"to":"a@example.com","subject":"Hi"}}""").get("jobId").asLong();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(id).get("job").get("state").asString()).isEqualTo("SUCCEEDED"));
        JsonNode detail = job(id);
        assertThat(detail.get("job").get("payload").get("to").asString()).isEqualTo("a@example.com");
        assertThat(detail.get("job").get("result").get("delivered").asBoolean()).isTrue();
        assertThat(detail.get("attempts")).hasSize(1);
        assertThat(detail.get("attempts").get(0).get("outcome").asString()).isEqualTo("SUCCEEDED");
    }

    @Test
    void aDeadJobCanBeRetriedFromTheDeadLetterView() throws Exception {
        long id = call("POST", "/api/jobs", """
                {"type":"always.fail","maxAttempts":1}""").get("jobId").asLong();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(id).get("job").get("state").asString()).isEqualTo("DEAD"));
        assertThat(job(id).get("job").get("lastError").asString()).contains("always fails");
        assertThat(call("GET", "/api/jobs?state=DEAD", null).get("jobs").toString()).contains("\"id\":" + id);

        call("POST", "/api/jobs/" + id + "/retry", "{}");
        // it fails again (the handler never succeeds) but the retry visibly ran: a second attempt is recorded
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(id).get("attempts").size()).isGreaterThanOrEqualTo(2));
        // retrying a job that is not dead is a conflict, not a silent no-op
        long ok = call("POST", "/api/jobs", """
                {"type":"email.send","payload":{"to":"b@example.com","subject":"x"}}""").get("jobId").asLong();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(ok).get("job").get("state").asString()).isEqualTo("SUCCEEDED"));
        assertThat(call("POST", "/api/jobs/" + ok + "/retry", "{}", true).statusCode()).isEqualTo(409);
        assertThat(call("GET", "/api/jobs/999999999", null, true).statusCode()).isEqualTo(404);
    }

    @Test
    void schedulesAreCreatedPreviewedTriggeredPausedAndRejectedWhenInvalid() throws Exception {
        JsonNode created = call("POST", "/api/schedules", """
                {"name":"berlin-test","cron":"30 7 * * 1-5","zone":"Europe/Berlin","jobType":"email.send",
                 "payload":{"to":"ops@example.com","subject":"Report"},"misfirePolicy":"SKIP"}""");
        long id = created.get("id").asLong();
        assertThat(created.get("upcoming")).hasSize(5);
        assertThat(created.get("upcoming").get(0).get("local").asString()).contains("07:30:00").contains("Europe/Berlin");

        assertThat(call("GET", "/api/cron/preview?cron=*/15 * * * *&zone=Asia/Tokyo&count=3".replace(" ", "%20"), null)).hasSize(3);

        long jobId = call("POST", "/api/schedules/" + id + "/trigger", "{}").get("jobId").asLong();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(jobId).get("job").get("state").asString()).isEqualTo("SUCCEEDED"));

        assertThat(call("POST", "/api/schedules/" + id + "/pause", "{}").get("paused").asBoolean()).isTrue();
        assertThat(call("POST", "/api/schedules", """
                {"name":"bad","cron":"61 * * * *","jobType":"email.send"}""", true).statusCode()).isEqualTo(400);
        assertThat(call("POST", "/api/schedules", """
                {"name":"badzone","cron":"* * * * *","zone":"Mars/Base","jobType":"email.send"}""", true).statusCode()).isEqualTo(400);
        call("DELETE", "/api/schedules/" + id, null);
        assertThat(call("GET", "/api/schedules", null).toString()).doesNotContain("berlin-test");
    }

    @Test
    void theOverviewStreamPushesSnapshotsAndMetricsAreExposed() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/stream"))
                .header("Authorization", AUTH).header("Accept", "text/event-stream").build();
        HttpResponse<java.io.InputStream> response = client.send(request, BodyHandlers.ofInputStream());
        assertThat(response.statusCode()).isEqualTo(200);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String event = null;
            String data = null;
            for (String line = reader.readLine(); line != null && data == null; line = reader.readLine()) {
                if (line.startsWith("event:")) {
                    event = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    data = line.substring(5).trim();
                }
            }
            assertThat(event).isEqualTo("overview");
            JsonNode overview = json.readTree(data);
            assertThat(overview.has("queues")).isTrue();
            assertThat(overview.get("workers").size()).isGreaterThanOrEqualTo(1);
        } catch (IOException e) {
            throw e;
        }
        // the demo handlers ran in other tests of this class, so the meters exist once any job finished
        long id = call("POST", "/api/jobs", """
                {"type":"email.send","payload":{"to":"m@example.com","subject":"m"}}""").get("jobId").asLong();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(
                () -> assertThat(job(id).get("job").get("state").asString()).isEqualTo("SUCCEEDED"));
        // queue depth gauges are refreshed every few seconds and appear once a queue is known
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            String metrics = call("GET", "/actuator/prometheus", null, true).body();
            assertThat(metrics).contains("pgscheduler_job_duration_seconds_count")
                    .contains("pgscheduler_job_latency_seconds_count")
                    .contains("pgscheduler_queue_depth{queue=\"default\",state=\"READY\"}");
        });
    }
}
