package io.github.mgeladzerezo.pgscheduler.autoconfigure;

import io.github.mgeladzerezo.pgscheduler.JobContext;
import io.github.mgeladzerezo.pgscheduler.JobHandler;
import io.github.mgeladzerezo.pgscheduler.JobPolicies;
import io.github.mgeladzerezo.pgscheduler.JobPolicy;
import io.github.mgeladzerezo.pgscheduler.RateLimit;
import io.github.mgeladzerezo.pgscheduler.worker.HandlerRegistry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;

/**
 * Finds the handlers in the application context once every singleton exists: beans implementing
 * {@link JobHandler} and bean methods annotated with
 * {@link io.github.mgeladzerezo.pgscheduler.annotation.JobHandler @JobHandler}. It runs before the
 * lifecycle phase that starts the worker, so the worker sees the complete set of job types when it starts.
 *
 * <p>Policy precedence for a job type, lowest to highest: library default, {@code pgscheduler.defaults},
 * annotation attributes, {@code pgscheduler.job-types.<type>}.
 */
final class JobHandlerRegistrar implements SmartInitializingSingleton {

    private final ConfigurableListableBeanFactory beanFactory;
    private final HandlerRegistry registry;
    private final JobPolicies policies;
    private final PgSchedulerProperties properties;

    JobHandlerRegistrar(ConfigurableListableBeanFactory beanFactory, HandlerRegistry registry, JobPolicies policies,
                        PgSchedulerProperties properties) {
        this.beanFactory = beanFactory;
        this.registry = registry;
        this.policies = policies;
        this.properties = properties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        for (Map.Entry<String, JobHandler> entry : beanFactory.getBeansOfType(JobHandler.class).entrySet()) {
            JobHandler<?> bean = entry.getValue();
            registry.register(bean, AopUtils.getTargetClass(bean));
            applyProperties(bean.jobType(), policies.forType(bean.jobType()));
        }
        for (String name : beanFactory.getBeanDefinitionNames()) {
            BeanDefinition definition = beanFactory.getMergedBeanDefinition(name);
            if (definition.isAbstract() || !definition.isSingleton()) {
                continue;
            }
            Class<?> type = beanFactory.getType(name, false);
            if (type == null || type.getName().startsWith("org.springframework.")) {
                continue;
            }
            Class<?> userClass = ClassUtils.getUserClass(type);
            Map<Method, io.github.mgeladzerezo.pgscheduler.annotation.JobHandler> methods = MethodIntrospector
                    .selectMethods(userClass, (MethodIntrospector.MetadataLookup<io.github.mgeladzerezo.pgscheduler.annotation.JobHandler>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method,
                                    io.github.mgeladzerezo.pgscheduler.annotation.JobHandler.class));
            if (!methods.isEmpty()) {
                Object bean = beanFactory.getBean(name);
                methods.forEach((method, annotation) -> registerMethod(bean, method, annotation));
            }
        }
    }

    private void registerMethod(Object bean, Method method,
                                io.github.mgeladzerezo.pgscheduler.annotation.JobHandler annotation) {
        Class<?>[] parameters = method.getParameterTypes();
        boolean first = parameters.length > 0 && !JobContext.class.isAssignableFrom(parameters[0]);
        boolean context = parameters.length > (first ? 1 : 0);
        int expected = (first ? 1 : 0) + (context ? 1 : 0);
        if (parameters.length != expected || (context && !JobContext.class.isAssignableFrom(parameters[expected - 1]))) {
            throw new IllegalStateException("@JobHandler method " + method + " must take (payload), (payload, JobContext),"
                    + " (JobContext) or no parameters");
        }
        Type payloadType = first ? ResolvableType.forMethodParameter(method, 0, ClassUtils.getUserClass(bean)).getType() : null;
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        invocable.setAccessible(true);
        String type = annotation.value();
        registry.register(type, payloadType, (payload, jobContext) -> {
            Object[] args = new Object[expected];
            if (first) {
                args[0] = payload;
            }
            if (context) {
                args[expected - 1] = jobContext;
            }
            try {
                return invocable.invoke(bean, args);
            } catch (InvocationTargetException e) {
                throw e.getCause() instanceof Exception cause ? cause : new IllegalStateException(e.getCause());
            }
        }, method.toGenericString());

        JobPolicy policy = policies.forType(type);
        if (annotation.maxAttempts() > 0) {
            policy = policy.withMaxAttempts(annotation.maxAttempts());
        }
        if (!annotation.timeout().isEmpty()) {
            policy = policy.withTimeout(DurationStyle.detectAndParse(annotation.timeout()));
        }
        if (annotation.noRetryFor().length > 0) {
            Set<Class<? extends Throwable>> noRetry = new LinkedHashSet<>(policy.noRetryFor());
            noRetry.addAll(Set.of(annotation.noRetryFor()));
            policy = policy.withNoRetryFor(noRetry);
        }
        if (annotation.rateLimitPerSecond() > 0) {
            policy = policy.withRateLimit(RateLimit.perSecond(annotation.rateLimitPerSecond()));
        }
        applyProperties(type, policy);
    }

    private void applyProperties(String type, JobPolicy policy) {
        policies.set(type, override(policy, properties.jobTypes().get(type)));
    }

    /** Applies the set values of {@code settings} on top of {@code base}. */
    static JobPolicy override(JobPolicy base, PgSchedulerProperties.JobType settings) {
        if (settings == null) {
            return base;
        }
        JobPolicy policy = base;
        if (settings.maxAttempts() != null) {
            policy = policy.withMaxAttempts(settings.maxAttempts());
        }
        if (settings.timeout() != null) {
            policy = policy.withTimeout(settings.timeout());
        }
        if (settings.backoffInitial() != null || settings.backoffMultiplier() != null
                || settings.backoffMax() != null || settings.backoffJitter() != null) {
            var backoff = policy.backoff();
            Duration initial = settings.backoffInitial() != null ? settings.backoffInitial() : backoff.initial();
            double multiplier = settings.backoffMultiplier() != null ? settings.backoffMultiplier() : backoff.multiplier();
            Duration max = settings.backoffMax() != null ? settings.backoffMax() : backoff.max();
            double jitter = settings.backoffJitter() != null ? settings.backoffJitter() : backoff.jitter();
            policy = policy.withBackoff(new io.github.mgeladzerezo.pgscheduler.Backoff(initial, multiplier, max, jitter));
        }
        if (settings.rateLimitPerSecond() != null) {
            policy = policy.withRateLimit(settings.rateLimitPerSecond() > 0 ? RateLimit.perSecond(settings.rateLimitPerSecond()) : null);
        }
        if (settings.noRetryFor() != null && !settings.noRetryFor().isEmpty()) {
            Set<Class<? extends Throwable>> noRetry = new LinkedHashSet<>(policy.noRetryFor());
            noRetry.addAll(settings.noRetryFor());
            policy = policy.withNoRetryFor(noRetry);
        }
        return policy;
    }
}
