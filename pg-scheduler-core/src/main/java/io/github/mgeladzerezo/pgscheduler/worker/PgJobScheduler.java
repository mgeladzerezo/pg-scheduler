package io.github.mgeladzerezo.pgscheduler.worker;

import io.github.mgeladzerezo.pgscheduler.EnqueueResult;
import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.JobRequest;
import io.github.mgeladzerezo.pgscheduler.JobScheduler;
import io.github.mgeladzerezo.pgscheduler.store.EnqueueDao;
import io.github.mgeladzerezo.pgscheduler.store.JobSpec;
import java.util.Collection;
import tools.jackson.databind.ObjectMapper;

/**
 * {@link JobScheduler} on PostgreSQL: resolves a request against the policy of its job type, serialises
 * the payload, and inserts through {@link EnqueueDao} on the caller's transaction.
 */
public final class PgJobScheduler implements JobScheduler {

    private final EnqueueDao dao;
    private final JobPolicies policies;
    private final ObjectMapper json;
    private final Continuations continuations;

    public PgJobScheduler(EnqueueDao dao, JobPolicies policies, ObjectMapper json) {
        this.dao = dao;
        this.policies = policies;
        this.json = json;
        this.continuations = new Continuations(json, policies);
    }

    @Override
    public EnqueueResult enqueue(JobRequest request) {
        return dao.enqueue(toSpec(request));
    }

    @Override
    public int enqueueAll(Collection<JobRequest> requests) {
        return dao.enqueueAll(requests.stream().map(this::toSpec).toList());
    }

    private JobSpec toSpec(JobRequest request) {
        JobPolicy policy = policies.forType(request.type());
        return new JobSpec(
                request.type(),
                request.queue(),
                json.writeValueAsString(request.payload()),
                request.priority(),
                request.runAt(),
                request.delay() == null ? 0 : request.delay().toMillis(),
                request.uniqueKey(),
                request.maxAttempts() != null ? request.maxAttempts() : policy.maxAttempts(),
                request.timeout() != null ? request.timeout().toMillis() : policy.timeout().toMillis(),
                continuations.serialise(request.continuation()),
                null);
    }
}
