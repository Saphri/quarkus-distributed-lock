package io.quarkiverse.lock.runtime;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.jboss.logging.Logger;

import io.quarkiverse.lock.LockLostEvent;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.LockRegistryName;
import io.quarkiverse.lock.spi.LockBackendFactory;
import io.smallrye.mutiny.infrastructure.Infrastructure;

/**
 * Creates and owns all registries of the application, lazily by name.
 */
@Singleton
public class LockRegistries {

    private static final Logger LOG = Logger.getLogger(LockRegistries.class);

    private final Map<String, LockRegistryImpl> registries = new ConcurrentHashMap<>();
    private final LockBuildTimeConfig buildConfig;
    private final LockRuntimeConfig config;
    private final Instance<LockBackendFactory> factories;
    private final Event<LockLostEvent> lostEvent;
    private final String instanceId;
    private final ScheduledExecutorService scheduler;

    @Inject
    public LockRegistries(LockBuildTimeConfig buildConfig, LockRuntimeConfig config,
            @Any Instance<LockBackendFactory> factories, Event<LockLostEvent> lostEvent) {
        this.buildConfig = buildConfig;
        this.config = config;
        this.factories = factories;
        this.lostEvent = lostEvent;
        this.instanceId = config.instanceId().orElseGet(LockRegistries::defaultInstanceId);
        AtomicInteger n = new AtomicInteger();
        ScheduledThreadPoolExecutor ex = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "lock-renewal-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        ex.setRemoveOnCancelPolicy(true);
        this.scheduler = Executors.unconfigurableScheduledExecutorService(ex);
    }

    static String defaultInstanceId() {
        String host = Optional.ofNullable(System.getenv("HOSTNAME")).filter(h -> !h.isBlank()).orElse("jvm");
        return host + "-" + ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String instanceId() {
        return instanceId;
    }

    public LockRegistry get(String name) {
        String n = name == null || name.isEmpty() ? LockRegistryName.DEFAULT : name;
        return registries.computeIfAbsent(n, this::create);
    }

    public LockRegistry getDefault() {
        return get(LockRegistryName.DEFAULT);
    }

    /** Registries created so far. */
    public Map<String, LockRegistryImpl> created() {
        return Map.copyOf(registries);
    }

    private LockRegistryImpl create(String name) {
        String type = buildConfig.type(name);
        LockBackendFactory factory = null;
        for (LockBackendFactory f : factories) {
            if (f.type().equals(type)) {
                factory = f;
                break;
            }
        }
        if (factory == null) {
            throw new IllegalStateException("No lock backend of type '" + type + "' for registry '" + name
                    + "'; add the extension providing it");
        }
        LockRegistryImpl registry = new LockRegistryImpl(name, factory.create(name), instanceId,
                config.waitTimeout(name), config.failOnLostLease(name), scheduler,
                Infrastructure.getDefaultWorkerPool(), lostEvent::fire);
        LOG.debugf("Created lock registry '%s' of type '%s' (instance id %s)", name, type, instanceId);
        return registry;
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
        registries.values().forEach(r -> {
            try {
                r.backend().close();
            } catch (RuntimeException e) {
                LOG.debugf(e, "Error closing lock backend of registry '%s'", r.name());
            }
        });
        registries.clear();
    }
}
