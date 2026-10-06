package io.github.mgeladzerezo.pgscheduler.worker;

import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.JobRequest;
import io.github.mgeladzerezo.pgscheduler.store.ClaimedJob;
import io.github.mgeladzerezo.pgscheduler.store.JobSpec;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Serialises a chain of continuations into the {@code continuation} column and turns the head of a stored
 * chain back into an insertable job when its predecessor succeeds.
 */
public final class Continuations {

    /** The stored form of one link; {@code then} is the rest of the chain. */
    record Link(String type, String queue, JsonNode payload, int priority, long delayMs, String uniqueKey,
                int maxAttempts, long timeoutMs, Link then) {
    }

    private final ObjectMapper json;
    private final JobPolicies policies;

    public Continuations(ObjectMapper json, JobPolicies policies) {
        this.json = json;
        this.policies = policies;
    }

    /** JSON for the chain starting at {@code request}, or {@code null} for no continuation. */
    public String serialise(JobRequest request) {
        return request == null ? null : json.writeValueAsString(toLink(request));
    }

    private Link toLink(JobRequest request) {
        JobPolicy policy = policies.forType(request.type());
        return new Link(request.type(), request.queue(), json.valueToTree(request.payload()), request.priority(),
                request.delay() == null ? 0 : request.delay().toMillis(), request.uniqueKey(),
                request.maxAttempts() != null ? request.maxAttempts() : policy.maxAttempts(),
                request.timeout() != null ? request.timeout().toMillis() : policy.timeout().toMillis(),
                request.continuation() == null ? null : toLink(request.continuation()));
    }

    /** The job to insert when {@code finished} succeeds, or {@code null} if it has no continuation. */
    public JobSpec next(ClaimedJob finished) {
        if (finished.continuationJson() == null) {
            return null;
        }
        Link link = json.readValue(finished.continuationJson(), Link.class);
        String payload = link.payload() == null ? "null" : link.payload().toString();
        return new JobSpec(link.type(), link.queue(), payload, link.priority(), null, link.delayMs(),
                link.uniqueKey(), link.maxAttempts(), link.timeoutMs(),
                link.then() == null ? null : json.writeValueAsString(link.then()), finished.id());
    }
}
