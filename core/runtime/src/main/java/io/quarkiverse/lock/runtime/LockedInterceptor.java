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
import io.quarkiverse.lock.runtime.LockedContract.BoundKey;
import io.quarkiverse.lock.runtime.LockedContract.Dispatch;
import io.quarkus.arc.ArcInvocationContext;
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
        String key = plan.key.resolve(ctx.getParameters());
        LockRegistry registry = plan.registry != null ? plan.registry : registries.get(plan.registryName);
        Duration wait = plan.waitTimeout != null ? plan.waitTimeout
                : ((LockRegistryImpl) registry).defaultWait();
        switch (plan.dispatch) {
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

    private Plan plan(Method method, Locked locked) {
        Dispatch dispatch = LockedContract.classify(method.getReturnType().getName());
        Duration wait = LockedContract.parseWait(locked.waitTimeout());
        String registryName = locked.registry();
        LockRegistry registry = registries.get(registryName);
        BoundKey key = LockedContract.bind(locked.value(), lockKeyNames(method));
        return new Plan(dispatch, wait, registryName, registry, locked.onTimeout(), key);
    }

    private static List<String> lockKeyNames(Method method) {
        Annotation[][] annotations = method.getParameterAnnotations();
        List<String> names = new ArrayList<>(annotations.length);
        for (Annotation[] parameter : annotations) {
            String name = null;
            for (Annotation a : parameter) {
                if (a instanceof LockKey k) {
                    name = k.value();
                }
            }
            names.add(name);
        }
        return names;
    }

    private record Plan(Dispatch dispatch, Duration waitTimeout, String registryName, LockRegistry registry,
            OnTimeout onTimeout, BoundKey key) {
    }
}
