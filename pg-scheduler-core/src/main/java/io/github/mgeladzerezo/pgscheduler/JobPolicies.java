package io.github.mgeladzerezo.pgscheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The per-job-type policy table: a default plus overrides by type. Thread-safe.
 */
public final class JobPolicies {

    private final JobPolicy defaults;
    private final Map<String, JobPolicy> byType = new ConcurrentHashMap<>();

    public JobPolicies(JobPolicy defaults) {
        this.defaults = defaults;
    }

    public JobPolicies() {
        this(JobPolicy.DEFAULT);
    }

    public JobPolicy defaults() {
        return defaults;
    }

    /** Sets the policy for one job type, replacing any earlier one. */
    public JobPolicies set(String jobType, JobPolicy policy) {
        byType.put(jobType, policy);
        return this;
    }

    /** The policy for a job type, or the default when none was set. */
    public JobPolicy forType(String jobType) {
        return byType.getOrDefault(jobType, defaults);
    }
}
