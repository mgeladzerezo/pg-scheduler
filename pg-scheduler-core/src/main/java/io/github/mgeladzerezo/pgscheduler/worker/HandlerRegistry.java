package io.github.mgeladzerezo.pgscheduler.worker;

import io.github.mgeladzerezo.pgscheduler.JobContext;
import io.github.mgeladzerezo.pgscheduler.JobHandler;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.core.ResolvableType;

/**
 * Maps job types to handlers and remembers the payload type each handler wants.
 *
 * <p>The payload type is kept as a {@link Type}, not a {@link Class}, so that a handler declared for
 * {@code List<OrderLine>} receives a list of {@code OrderLine} and not a list of maps.
 */
public final class HandlerRegistry {

    /** Uniform call shape for both handler styles. The return value, if any, becomes the job result. */
    @FunctionalInterface
    public interface Invoker {
        Object invoke(Object payload, JobContext context) throws Exception;
    }

    /**
     * @param payloadType type to deserialise the payload into, or {@code null} if the handler ignores it
     * @param description what will be invoked, for logs and error messages
     */
    public record Registration(String jobType, Type payloadType, Invoker invoker, String description) {
    }

    private final Map<String, Registration> handlers = new ConcurrentHashMap<>();

    /**
     * Registers a handler.
     *
     * @throws IllegalStateException if the job type already has one: two handlers for one type would make
     *                               it a matter of chance which one runs
     */
    public void register(String jobType, Type payloadType, Invoker invoker, String description) {
        if (jobType == null || jobType.isBlank()) {
            throw new IllegalArgumentException("job type must not be blank (" + description + ")");
        }
        Registration existing = handlers.putIfAbsent(jobType, new Registration(jobType, payloadType, invoker, description));
        if (existing != null) {
            throw new IllegalStateException("Two handlers for job type '" + jobType + "': "
                    + existing.description() + " and " + description);
        }
    }

    /** Registers a handler with an explicit payload class; the way to register a lambda. */
    public <T> void register(String jobType, Class<T> payloadType, PayloadHandler<T> handler) {
        register(jobType, payloadType, (payload, context) -> {
            handler.handle(payloadType.cast(payload), context);
            return null;
        }, "lambda for " + jobType);
    }

    /** The lambda-friendly shape of {@link JobHandler#handle}. */
    @FunctionalInterface
    public interface PayloadHandler<T> {
        void handle(T payload, JobContext context) throws Exception;
    }

    /**
     * Registers a {@link JobHandler}, reading its payload type from the generic signature.
     *
     * @throws IllegalArgumentException if the type argument cannot be resolved, which happens for raw types
     *                                  and lambdas (the compiler erases a lambda's type argument)
     */
    public void register(JobHandler<?> handler) {
        register(handler, handler.getClass());
    }

    /**
     * As {@link #register(JobHandler)}, but resolves generics against {@code declaredClass}; used when
     * {@code handler} is a proxy and the user's class is known.
     */
    @SuppressWarnings("unchecked")
    public void register(JobHandler<?> handler, Class<?> declaredClass) {
        ResolvableType generic = ResolvableType.forClass(declaredClass).as(JobHandler.class).getGeneric(0);
        Type payloadType = generic.getType();
        if (generic.resolve() == null || generic.hasUnresolvableGenerics()) {
            throw new IllegalArgumentException("Cannot resolve the payload type of " + declaredClass.getName()
                    + ". Implement JobHandler<YourPayload> in a named class, or register with an explicit type.");
        }
        JobHandler<Object> typed = (JobHandler<Object>) handler;
        register(handler.jobType(), payloadType, (payload, context) -> {
            typed.handle(payload, context);
            return null;
        }, declaredClass.getName());
    }

    /** The handler for a job type, or {@code null}. */
    public Registration find(String jobType) {
        return handlers.get(jobType);
    }

    /** Job types this node can run. */
    public Set<String> jobTypes() {
        return Set.copyOf(handlers.keySet());
    }
}
