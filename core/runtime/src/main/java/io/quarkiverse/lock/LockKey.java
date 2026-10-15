package io.quarkiverse.lock;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Marks a parameter of a {@link Locked} method as the value of the {@code {name}} placeholder in the lock key. The
 * parameter's {@code toString()} is used; {@code null} becomes {@code "null"}.
 */
@Target(PARAMETER)
@Retention(RUNTIME)
public @interface LockKey {

    /** The placeholder name. */
    String value();
}
