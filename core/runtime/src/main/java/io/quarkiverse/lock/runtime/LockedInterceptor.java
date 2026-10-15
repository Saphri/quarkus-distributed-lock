package io.quarkiverse.lock.runtime;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockTimeoutException;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.quarkus.arc.ArcInvocationContext;
import io.quarkus.runtime.configuration.DurationConverter;
import io.smallrye.mutiny.Uni;

/**
 * {@code registry.executeLocked(key, wait, () -> ctx.proceed())}. Runs before {@code @Transactional}, so the
 * transaction commits before the lock is released.
 */
@Locked("")
@Interceptor
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 100)
public class LockedInterceptor {

    private final Map<Method, Plan> plans = new ConcurrentHashMap<>();

    @Inject
    LockRegistries registries;

    @AroundInvoke
    Object lock(InvocationContext ctx) throws Exception {
        Plan plan = plans.computeIfAbsent(ctx.getMethod(), m -> plan(m, binding(ctx)));
        String key = plan.key(ctx.getParameters());
        LockRegistry registry = plan.registry != null ? plan.registry : registries.get(plan.registryName);
        Duration wait = plan.waitTimeout != null ? plan.waitTimeout
                : ((LockRegistryImpl) registry).defaultWait();
        switch (plan.kind) {
            case UNI:
                return lockedUni(registry, key, wait, plan, ctx);
            case COMPLETION_STAGE:
                Uni<Object> u = lockedUni(registry, key, wait, plan,
                        () -> Uni.createFrom().completionStage(proceedStage(ctx)));
                return u.subscribeAsCompletionStage();
            default:
                if (plan.onTimeout == OnTimeout.SKIP) {
                    registry.tryExecuteLocked(key, wait, ctx::proceed);
                    return null;
                }
                return registry.executeLocked(key, wait, ctx::proceed);
        }
    }

    @SuppressWarnings("unchecked")
    private Uni<Object> lockedUni(LockRegistry registry, String key, Duration wait, Plan plan, InvocationContext ctx) {
        return lockedUni(registry, key, wait, plan, () -> {
            try {
                return (Uni<Object>) ctx.proceed();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        });
    }

    private Uni<Object> lockedUni(LockRegistry registry, String key, Duration wait, Plan plan,
            java.util.function.Supplier<Uni<Object>> task) {
        Uni<Object> u = registry.executeLockedAsync(key, wait, task);
        if (plan.onTimeout == OnTimeout.SKIP) {
            u = u.onFailure(LockTimeoutException.class).recoverWithNull();
        }
        return u;
    }

    @SuppressWarnings("unchecked")
    private static CompletionStage<Object> proceedStage(InvocationContext ctx) {
        try {
            return (CompletionStage<Object>) ctx.proceed();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static Locked binding(InvocationContext ctx) {
        Locked locked = null;
        if (ctx instanceof ArcInvocationContext arc) {
            locked = arc.findIterceptorBinding(Locked.class);
        }
        if (locked == null) {
            locked = ctx.getMethod().getAnnotation(Locked.class);
        }
        if (locked == null) {
            locked = ctx.getMethod().getDeclaringClass().getAnnotation(Locked.class);
        }
        if (locked == null) {
            throw new IllegalStateException("No @Locked binding found on " + ctx.getMethod());
        }
        return locked;
    }

    enum Kind {
        SYNC,
        UNI,
        COMPLETION_STAGE
    }

    private Plan plan(Method method, Locked locked) {
        Kind kind = Uni.class.isAssignableFrom(method.getReturnType()) ? Kind.UNI
                : CompletionStage.class.isAssignableFrom(method.getReturnType()) ? Kind.COMPLETION_STAGE : Kind.SYNC;
        Duration wait = locked.waitTimeout().isEmpty() ? null : DurationConverter.parseDuration(locked.waitTimeout());
        String registryName = locked.registry();
        LockRegistry registry = registries.get(registryName);
        KeyTemplate template = KeyTemplate.parse(locked.value(), method);
        return new Plan(kind, wait, registryName, registry, locked.onTimeout(), template);
    }

    private record Plan(Kind kind, Duration waitTimeout, String registryName, LockRegistry registry, OnTimeout onTimeout,
            KeyTemplate template) {

        String key(Object[] params) {
            return template.render(params);
        }
    }

    /**
     * A key with {@code {name}} placeholders bound to {@link LockKey} parameter positions. The same parsing rules are
     * enforced at build time.
     */
    record KeyTemplate(List<String> literals, int[] paramIndexes) {

        static KeyTemplate parse(String template, Method method) {
            Map<String, Integer> params = new ConcurrentHashMap<>();
            Annotation[][] annotations = method.getParameterAnnotations();
            for (int i = 0; i < annotations.length; i++) {
                for (Annotation a : annotations[i]) {
                    if (a instanceof LockKey k) {
                        params.put(k.value(), i);
                    }
                }
            }
            List<String> literals = new ArrayList<>();
            List<Integer> indexes = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            int i = 0;
            while (i < template.length()) {
                char c = template.charAt(i);
                if (c == '{') {
                    int end = template.indexOf('}', i);
                    if (end < 0) {
                        throw new IllegalArgumentException("Unclosed placeholder in lock key '" + template + "'");
                    }
                    String name = template.substring(i + 1, end);
                    Integer idx = params.get(name);
                    if (idx == null) {
                        throw new IllegalArgumentException("Lock key '" + template + "' on " + method
                                + " references {" + name + "} but no parameter is annotated @LockKey(\"" + name + "\")");
                    }
                    literals.add(current.toString());
                    current.setLength(0);
                    indexes.add(idx);
                    i = end + 1;
                } else {
                    current.append(c);
                    i++;
                }
            }
            literals.add(current.toString());
            return new KeyTemplate(List.copyOf(literals), indexes.stream().mapToInt(Integer::intValue).toArray());
        }

        String render(Object[] params) {
            if (paramIndexes.length == 0) {
                return literals.get(0);
            }
            StringBuilder sb = new StringBuilder(literals.get(0));
            for (int i = 0; i < paramIndexes.length; i++) {
                sb.append(params[paramIndexes[i]]).append(literals.get(i + 1));
            }
            return sb.toString();
        }
    }
}
