package io.quarkiverse.lock;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;

/**
 * Runs the annotated method while holding a lock: equivalent to
 * {@code registry.executeLocked(key, waitTimeout, () -> invocation.proceed())}.
 * <p>
 * Methods returning {@code Uni} or {@code CompletionStage} are locked until the returned result completes, not until
 * the method returns. The usual CDI interceptor rules apply: self-invocation and private methods are not intercepted.
 */
@Inherited
@InterceptorBinding
@Target({ METHOD, TYPE })
@Retention(RUNTIME)
public @interface Locked {

    /**
     * The lock key. May contain {@code {name}} placeholders filled from parameters annotated with
     * {@code @LockKey("name")}; placeholders are validated at build time. Class-level keys cannot contain placeholders.
     */
    @Nonbinding
    String value();

    /**
     * How long to wait for the lock, in Quarkus duration syntax ({@code "5s"}, {@code "PT5S"}, {@code "0s"} for a
     * single attempt). Empty means the registry's configured {@code wait-timeout}. Validated at build time.
     */
    @Nonbinding
    String waitTimeout() default "";

    /**
     * The registry to use; empty means the default registry.
     */
    @Nonbinding
    String registry() default "";

    /**
     * What to do when the lock cannot be acquired in time.
     */
    @Nonbinding
    OnTimeout onTimeout() default OnTimeout.THROW;
}
