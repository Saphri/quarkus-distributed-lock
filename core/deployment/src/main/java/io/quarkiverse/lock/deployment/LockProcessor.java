package io.quarkiverse.lock.deployment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import io.quarkiverse.lock.runtime.LockedContract;
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

class LockProcessor {

    private static final Logger LOG = Logger.getLogger(LockProcessor.class);

    static final String FEATURE = "distributed-lock";

    static final DotName LOCKED = DotName.createSimple(Locked.class);
    static final DotName LOCK_KEY = DotName.createSimple(LockKey.class);
    static final DotName LOCK_REGISTRY_NAME = DotName.createSimple(LockRegistryName.class);
    static final DotName LOCK_BACKEND_FACTORY = DotName.createSimple(LockBackendFactory.class);

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
                validate(locked, method, problems);
                reflective.produce(new ReflectiveMethodBuildItem("@Locked key placeholders", method));
            } else if (target.kind() == AnnotationTarget.Kind.CLASS) {
                ClassInfo clazz = target.asClass();
                String where = "@Locked on class " + clazz.name();
                report(where, LockedContract.validateKey(keyOf(locked), List.of(), true), problems);
                validateWait(locked, where, problems);
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

    private static void validate(AnnotationInstance locked, MethodInfo method, List<Throwable> problems) {
        String where = "@Locked on " + method.declaringClass().name() + "#" + method.name();
        if (java.lang.reflect.Modifier.isPrivate(method.flags())) {
            problems.add(new IllegalStateException(where + ": private methods are not intercepted"));
        }
        if (java.lang.reflect.Modifier.isStatic(method.flags())) {
            problems.add(new IllegalStateException(where + ": static methods are not intercepted"));
        }
        List<String> lockKeyNames = new ArrayList<>();
        for (MethodParameterInfo p : method.parameters()) {
            AnnotationInstance lk = p.declaredAnnotation(LOCK_KEY);
            lockKeyNames.add(lk == null ? null : lk.value().asString());
        }
        report(where, LockedContract.validateKey(keyOf(locked), lockKeyNames, false), problems);
        validateWait(locked, where, problems);
        validateReturnType(locked, method, where, problems);
    }

    private static String keyOf(AnnotationInstance locked) {
        return locked.value() == null ? null : locked.value().asString();
    }

    private static void validateWait(AnnotationInstance locked, String where, List<Throwable> problems) {
        AnnotationValue wait = locked.value("waitTimeout");
        try {
            LockedContract.parseWait(wait == null ? null : wait.asString());
        } catch (IllegalArgumentException e) {
            problems.add(new IllegalStateException(where + ": " + e.getMessage(), e.getCause()));
        }
    }

    private static void validateReturnType(AnnotationInstance locked, MethodInfo method, String where,
            List<Throwable> problems) {
        Type rt = method.returnType();
        String typeArgument = rt.kind() == Type.Kind.PARAMETERIZED_TYPE
                ? rt.asParameterizedType().arguments().get(0).name().toString()
                : null;
        AnnotationValue onTimeout = locked.value("onTimeout");
        OnTimeout mode = onTimeout == null ? OnTimeout.THROW : OnTimeout.valueOf(onTimeout.asEnum());
        report(where, LockedContract.validateReturnType(rt.name().toString(), typeArgument, mode), problems);
    }

    private static void report(String where, List<String> messages, List<Throwable> problems) {
        for (String message : messages) {
            problems.add(new IllegalStateException(where + ": " + message));
        }
    }
}
