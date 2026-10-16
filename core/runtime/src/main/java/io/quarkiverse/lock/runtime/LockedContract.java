package io.quarkiverse.lock.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.quarkus.runtime.configuration.DurationConverter;

/**
 * The rules of {@link Locked}: key placeholder grammar, {@link LockKey} binding, return-type dispatch and
 * {@code waitTimeout} parsing. Build-time validation and the runtime interceptor both call this class, so the two
 * cannot drift apart. It works on plain names and strings so that Jandex and reflection callers can share it.
 */
public final class LockedContract {

    public static final String VOID = "void";
    public static final String VOID_CLASS = "java.lang.Void";
    public static final String UNI = "io.smallrye.mutiny.Uni";
    public static final String COMPLETION_STAGE = "java.util.concurrent.CompletionStage";
    public static final String COMPLETABLE_FUTURE = "java.util.concurrent.CompletableFuture";
    static final Set<String> STREAMS = Set.of("io.smallrye.mutiny.Multi", "org.reactivestreams.Publisher",
            "java.util.concurrent.Flow$Publisher");

    private LockedContract() {
    }

    /**
     * A lock key split into literals and {@code {name}} placeholders: {@code literals.size() == placeholders.size() + 1}.
     */
    public record KeyTemplate(List<String> literals, List<String> placeholders) {
    }

    /**
     * Parses the placeholder grammar: {@code {name}} placeholders with non-empty names, no nesting, no stray braces.
     *
     * @throws IllegalArgumentException if the key is not well formed
     */
    public static KeyTemplate parse(String key) {
        List<String> literals = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < key.length()) {
            char c = key.charAt(i);
            if (c == '}') {
                throw new IllegalArgumentException("unbalanced '}' in lock key '" + key + "'");
            }
            if (c != '{') {
                current.append(c);
                i++;
                continue;
            }
            int end = key.indexOf('}', i);
            if (end < 0) {
                throw new IllegalArgumentException("unbalanced braces in lock key '" + key + "'");
            }
            String name = key.substring(i + 1, end);
            if (name.isEmpty() || name.indexOf('{') >= 0) {
                throw new IllegalArgumentException("invalid placeholder {" + name + "} in lock key '" + key + "'");
            }
            literals.add(current.toString());
            current.setLength(0);
            placeholders.add(name);
            i = end + 1;
        }
        literals.add(current.toString());
        return new KeyTemplate(List.copyOf(literals), List.copyOf(placeholders));
    }

    /**
     * Every problem with a {@link Locked} key and the {@link LockKey} parameters it is declared with.
     *
     * @param key the {@link Locked#value()}
     * @param lockKeyNames the {@link LockKey#value()} of each method parameter by position, {@code null} for parameters
     *        without {@code @LockKey}; ignored for class-level keys
     * @param classLevel whether the annotation is on a class rather than a method
     */
    public static List<String> validateKey(String key, List<String> lockKeyNames, boolean classLevel) {
        List<String> problems = new ArrayList<>();
        if (key == null || key.isBlank()) {
            problems.add("the lock key must not be empty");
            return problems;
        }
        KeyTemplate template;
        try {
            template = parse(key);
        } catch (IllegalArgumentException e) {
            problems.add(e.getMessage());
            template = null;
        }
        if (classLevel) {
            if (template != null && !template.placeholders().isEmpty()) {
                problems.add("class-level lock keys cannot contain placeholders; annotate the methods instead");
            }
            return problems;
        }
        Set<String> declared = new HashSet<>();
        for (String name : lockKeyNames) {
            if (name == null) {
                continue;
            }
            if (name.isEmpty() || name.contains("{") || name.contains("}")) {
                problems.add("invalid @LockKey name '" + name + "'");
            }
            if (!declared.add(name)) {
                problems.add("duplicate @LockKey(\"" + name + "\")");
            }
        }
        if (template == null) {
            return problems;
        }
        for (String name : template.placeholders()) {
            if (!declared.contains(name)) {
                problems.add(unknownPlaceholder(key, name));
            }
        }
        for (String name : declared) {
            if (!template.placeholders().contains(name)) {
                problems.add("@LockKey(\"" + name + "\") is not used in lock key '" + key + "'");
            }
        }
        return problems;
    }

    /**
     * Binds a key to the parameters that fill its placeholders.
     *
     * @param lockKeyNames as for {@link #validateKey}
     * @throws IllegalArgumentException if the key is malformed or references an undeclared placeholder
     */
    public static BoundKey bind(String key, List<String> lockKeyNames) {
        KeyTemplate template = parse(key);
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < lockKeyNames.size(); i++) {
            if (lockKeyNames.get(i) != null) {
                positions.putIfAbsent(lockKeyNames.get(i), i);
            }
        }
        int[] indexes = new int[template.placeholders().size()];
        for (int i = 0; i < indexes.length; i++) {
            String name = template.placeholders().get(i);
            Integer position = positions.get(name);
            if (position == null) {
                throw new IllegalArgumentException(unknownPlaceholder(key, name));
            }
            indexes[i] = position;
        }
        return new BoundKey(template.literals(), indexes);
    }

    private static String unknownPlaceholder(String key, String name) {
        return "lock key '" + key + "' references {" + name + "} but no parameter is annotated @LockKey(\"" + name
                + "\")";
    }

    /**
     * A key template whose placeholders are resolved to parameter positions.
     */
    public record BoundKey(List<String> literals, int[] paramIndexes) {

        /** Renders the key; each placeholder becomes its parameter's {@code toString()}, or {@code "null"}. */
        public String resolve(Object[] params) {
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

    /**
     * How the interceptor holds the lock for a method.
     */
    public enum Dispatch {
        /** Until the method returns. */
        SYNC,
        /** Until the returned {@code Uni} completes. */
        UNI,
        /** Until the returned {@code CompletionStage} completes. */
        COMPLETION_STAGE
    }

    /**
     * The dispatch kind for a method's declared return type.
     *
     * @param returnType the binary name of the raw return type, {@code "void"} for void
     * @throws IllegalArgumentException for streams, which cannot be locked declaratively
     */
    public static Dispatch classify(String returnType) {
        if (STREAMS.contains(returnType)) {
            String simple = returnType.substring(returnType.lastIndexOf('.') + 1);
            throw new IllegalArgumentException("streams (" + simple
                    + ") are not supported; return Uni or CompletionStage, or lock programmatically");
        }
        if (returnType.equals(UNI)) {
            return Dispatch.UNI;
        }
        if (returnType.equals(COMPLETION_STAGE) || returnType.equals(COMPLETABLE_FUTURE)) {
            return Dispatch.COMPLETION_STAGE;
        }
        return Dispatch.SYNC;
    }

    /**
     * Every problem with a method's return type under the given {@link OnTimeout}.
     *
     * @param returnType as for {@link #classify}
     * @param firstTypeArgument the binary name of the return type's first type argument, or {@code null} if it has none
     */
    public static List<String> validateReturnType(String returnType, String firstTypeArgument, OnTimeout onTimeout) {
        Dispatch dispatch;
        try {
            dispatch = classify(returnType);
        } catch (IllegalArgumentException e) {
            return List.of(e.getMessage());
        }
        if (onTimeout == OnTimeout.SKIP) {
            boolean ok = dispatch == Dispatch.SYNC ? returnType.equals(VOID) : VOID_CLASS.equals(firstTypeArgument);
            if (!ok) {
                String found = firstTypeArgument == null ? returnType : returnType + "<" + firstTypeArgument + ">";
                return List.of("onTimeout = SKIP requires a void, Uni<Void> or CompletionStage<Void> return type, found "
                        + found);
            }
        }
        return List.of();
    }

    /**
     * Parses {@link Locked#waitTimeout()}.
     *
     * @return the wait timeout, or {@code null} when empty, meaning the registry's configured default
     * @throws IllegalArgumentException if the value is not a valid, non-negative duration
     */
    public static Duration parseWait(String waitTimeout) {
        if (waitTimeout == null || waitTimeout.isEmpty()) {
            return null;
        }
        Duration wait;
        try {
            wait = DurationConverter.parseDuration(waitTimeout);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid waitTimeout '" + waitTimeout + "'", e);
        }
        if (wait == null) {
            throw new IllegalArgumentException("invalid waitTimeout '" + waitTimeout + "'");
        }
        if (wait.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must not be negative");
        }
        return wait;
    }

    /** {@link #parseWait} with the registry default applied. */
    public static Duration resolveWait(Duration annotated, Duration registryDefault) {
        return annotated != null ? annotated : registryDefault;
    }
}
