package io.quarkiverse.lock;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.enterprise.util.Nonbinding;
import jakarta.inject.Qualifier;

/**
 * Selects a named {@link LockRegistry}: {@code @Inject @LockRegistryName("jobs") LockRegistry jobs;}. A registry is
 * configured under {@code quarkus.lock."name".*} and falls back to the {@code quarkus.lock.*} defaults.
 */
@Qualifier
@Target({ FIELD, METHOD, PARAMETER })
@Retention(RUNTIME)
public @interface LockRegistryName {

    /** Name of the default registry. */
    String DEFAULT = "<default>";

    @Nonbinding
    String value();

    final class Literal extends AnnotationLiteral<LockRegistryName> implements LockRegistryName {

        private final String value;

        public Literal(String value) {
            this.value = value;
        }

        public static Literal of(String value) {
            return new Literal(value);
        }

        @Override
        public String value() {
            return value;
        }
    }
}
