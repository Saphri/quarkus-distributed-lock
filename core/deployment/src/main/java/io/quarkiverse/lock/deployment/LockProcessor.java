package io.quarkiverse.lock.deployment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.MethodParameterInfo;
import org.jboss.jandex.Type;
import org.jboss.logging.Logger;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.quarkiverse.lock.runtime.InMemoryLockBackendFactory;
import io.quarkiverse.lock.spi.LockBackendFactory;
import io.quarkiverse.lock.runtime.LockBuildTimeConfig;
import io.quarkiverse.lock.runtime.LockRecorder;
import io.quarkiverse.lock.runtime.LockRegistries;
import io.quarkiverse.lock.runtime.LockRegistryProducer;
import io.quarkiverse.lock.runtime.LockedInterceptor;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.ValidationPhaseBuildItem.ValidationErrorBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.AdditionalIndexedClassesBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveMethodBuildItem;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.quarkus.runtime.configuration.DurationConverter;

class LockProcessor {

    private static final Logger LOG = Logger.getLogger(LockProcessor.class);

    static final String FEATURE = "distributed-lock";

    static final DotName LOCKED = DotName.createSimple(Locked.class);
    static final DotName LOCK_KEY = DotName.createSimple(LockKey.class);
    static final DotName LOCK_REGISTRY_NAME = DotName.createSimple(LockRegistryName.class);
    static final DotName LOCK_BACKEND_FACTORY = DotName.createSimple(LockBackendFactory.class);
    static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");
    static final DotName PUBLISHER = DotName.createSimple("org.reactivestreams.Publisher");
    static final DotName FLOW_PUBLISHER = DotName.createSimple("java.util.concurrent.Flow$Publisher");
    static final DotName COMPLETION_STAGE = DotName.createSimple("java.util.concurrent.CompletionStage");
    static final DotName COMPLETABLE_FUTURE = DotName.createSimple("java.util.concurrent.CompletableFuture");
    static final DotName VOID = DotName.createSimple(Void.class);

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalIndexedClassesBuildItem indexApi() {
        return new AdditionalIndexedClassesBuildItem(Locked.class.getName(), LockKey.class.getName(),
                LockRegistryName.class.getName());
    }

    @BuildStep
    AdditionalBeanBuildItem beans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(LockRegistries.class, LockRegistryProducer.class, InMemoryLockBackendFactory.class,
                        LockedInterceptor.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    void knownRegistries(LockBuildTimeConfig config, CombinedIndexBuildItem combinedIndex, LockRecorder recorder) {
        Set<String> names = new TreeSet<>(config.registries().keySet());
        names.add(LockRegistryName.DEFAULT);
        IndexView index = combinedIndex.getIndex();
        for (AnnotationInstance a : index.getAnnotations(LOCK_REGISTRY_NAME)) {
            names.add(a.value().asString());
        }
        for (AnnotationInstance a : index.getAnnotations(LOCKED)) {
            AnnotationValue registry = a.value("registry");
            if (registry != null && !registry.asString().isEmpty()) {
                names.add(registry.asString());
            }
        }
        names.removeIf(String::isEmpty);
        recorder.setKnownRegistries(names);
    }

    @BuildStep
    LockBackendTypeBuildItem inMemoryBackend() {
        return new LockBackendTypeBuildItem(LockBuildTimeConfig.IN_MEMORY);
    }

    @BuildStep
    void validateBackendTypes(LockBuildTimeConfig config, List<LockBackendTypeBuildItem> available,
            CombinedIndexBuildItem combinedIndex, BuildProducer<ValidationErrorBuildItem> validationErrors) {
        Set<String> types = available.stream().map(LockBackendTypeBuildItem::getType)
                .collect(Collectors.toCollection(TreeSet::new));
        // backends implemented in the application itself announce their type only at runtime
        Set<String> appFactories = combinedIndex.getIndex().getAllKnownImplementations(LOCK_BACKEND_FACTORY).stream()
                .map(c -> c.name().toString()).collect(Collectors.toCollection(TreeSet::new));
        Set<String> names = new HashSet<>(config.registries().keySet());
        names.add(LockRegistryName.DEFAULT);
        List<String> errors = new ArrayList<>();
        for (String name : names) {
            String type = config.type(name);
            if (types.contains(type)) {
                continue;
            }
            String msg = "Lock registry '" + name + "' uses unknown type '" + type + "'; available types: " + types
                    + ". Add the extension providing it (for example quarkus-distributed-lock-nats).";
            if (appFactories.isEmpty()) {
                errors.add(msg);
            } else {
                LOG.debugf("%s Not failing the build because the application declares LockBackendFactory beans %s",
                        msg, appFactories);
            }
        }
        if (!errors.isEmpty()) {
            validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(String.join("\n", errors))));
        }
    }

    @BuildStep
    void validateLockedUsage(CombinedIndexBuildItem combinedIndex, BuildProducer<ValidationErrorBuildItem> errors,
            BuildProducer<ReflectiveMethodBuildItem> reflective) {
        IndexView index = combinedIndex.getIndex();
        List<Throwable> problems = new ArrayList<>();
        for (AnnotationInstance locked : index.getAnnotations(LOCKED)) {
            AnnotationTarget target = locked.target();
            if (target.kind() == AnnotationTarget.Kind.METHOD) {
                MethodInfo method = target.asMethod();
                validate(locked, method, false, problems);
                reflective.produce(new ReflectiveMethodBuildItem("@Locked key placeholders", method));
            } else if (target.kind() == AnnotationTarget.Kind.CLASS) {
                ClassInfo clazz = target.asClass();
                String where = "@Locked on class " + clazz.name();
                if (locked.value().asString().contains("{")) {
                    problems.add(new IllegalStateException(where
                            + ": class-level lock keys cannot contain placeholders; annotate the methods instead"));
                }
                validateCommon(locked, where, problems);
                for (MethodInfo method : clazz.methods()) {
                    if (method.isConstructor() || method.isStaticInitializer() || method.isSynthetic()
                            || java.lang.reflect.Modifier.isStatic(method.flags())
                            || java.lang.reflect.Modifier.isPrivate(method.flags())
                            || method.hasDeclaredAnnotation(LOCKED)) {
                        continue;
                    }
                    validateReturnType(locked, method, where + ", method " + method.name(), problems);
                }
            }
        }
        for (Throwable p : problems) {
            errors.produce(new ValidationErrorBuildItem(p));
        }
    }

    private static void validate(AnnotationInstance locked, MethodInfo method, boolean fromClass, List<Throwable> problems) {
        String where = "@Locked on " + method.declaringClass().name() + "#" + method.name();
        if (java.lang.reflect.Modifier.isPrivate(method.flags())) {
            problems.add(new IllegalStateException(where + ": private methods are not intercepted"));
        }
        if (java.lang.reflect.Modifier.isStatic(method.flags())) {
            problems.add(new IllegalStateException(where + ": static methods are not intercepted"));
        }
        validateCommon(locked, where, problems);
        validateReturnType(locked, method, where, problems);

        Map<String, Integer> keyParams = new HashMap<>();
        for (MethodParameterInfo p : method.parameters()) {
            AnnotationInstance lk = p.declaredAnnotation(LOCK_KEY);
            if (lk == null) {
                continue;
            }
            String name = lk.value().asString();
            if (name.isEmpty() || name.contains("{") || name.contains("}")) {
                problems.add(new IllegalStateException(where + ": invalid @LockKey name '" + name + "'"));
            }
            if (keyParams.put(name, (int) p.position()) != null) {
                problems.add(new IllegalStateException(where + ": duplicate @LockKey(\"" + name + "\")"));
            }
        }
        String key = locked.value().asString();
        Set<String> used = new HashSet<>();
        int i = 0;
        while (i < key.length()) {
            int open = key.indexOf('{', i);
            int close = key.indexOf('}', i);
            if (open < 0) {
                if (close >= 0) {
                    problems.add(new IllegalStateException(where + ": unbalanced '}' in lock key '" + key + "'"));
                }
                break;
            }
            int end = key.indexOf('}', open);
            if (end < 0 || (close >= 0 && close < open)) {
                problems.add(new IllegalStateException(where + ": unbalanced braces in lock key '" + key + "'"));
                break;
            }
            String name = key.substring(open + 1, end);
            if (!keyParams.containsKey(name)) {
                problems.add(new IllegalStateException(where + ": lock key '" + key + "' references {" + name
                        + "} but no parameter is annotated @LockKey(\"" + name + "\")"));
            }
            used.add(name);
            i = end + 1;
        }
        for (String name : keyParams.keySet()) {
            if (!used.contains(name)) {
                problems.add(new IllegalStateException(where + ": @LockKey(\"" + name + "\") is not used in lock key '"
                        + key + "'"));
            }
        }
    }

    private static void validateCommon(AnnotationInstance locked, String where, List<Throwable> problems) {
        if (locked.value() == null || locked.value().asString().isBlank()) {
            problems.add(new IllegalStateException(where + ": the lock key must not be empty"));
        }
        AnnotationValue wait = locked.value("waitTimeout");
        if (wait != null && !wait.asString().isEmpty()) {
            try {
                if (DurationConverter.parseDuration(wait.asString()).isNegative()) {
                    problems.add(new IllegalStateException(where + ": waitTimeout must not be negative"));
                }
            } catch (RuntimeException e) {
                problems.add(new IllegalStateException(where + ": invalid waitTimeout '" + wait.asString() + "'", e));
            }
        }
    }

    private static void validateReturnType(AnnotationInstance locked, MethodInfo method, String where,
            List<Throwable> problems) {
        Type rt = method.returnType();
        DotName raw = rt.name();
        if (raw.equals(MULTI) || raw.equals(PUBLISHER) || raw.equals(FLOW_PUBLISHER)) {
            problems.add(new IllegalStateException(where + ": streams (" + raw.withoutPackagePrefix()
                    + ") are not supported; return Uni or CompletionStage, or lock programmatically"));
            return;
        }
        AnnotationValue onTimeout = locked.value("onTimeout");
        if (onTimeout != null && OnTimeout.valueOf(onTimeout.asEnum()) == OnTimeout.SKIP) {
            boolean ok = rt.kind() == Type.Kind.VOID
                    || ((raw.equals(UNI) || raw.equals(COMPLETION_STAGE) || raw.equals(COMPLETABLE_FUTURE))
                            && rt.kind() == Type.Kind.PARAMETERIZED_TYPE
                            && rt.asParameterizedType().arguments().get(0).name().equals(VOID));
            if (!ok) {
                problems.add(new IllegalStateException(where
                        + ": onTimeout = SKIP requires a void, Uni<Void> or CompletionStage<Void> return type, found "
                        + rt));
            }
        }
    }
}
