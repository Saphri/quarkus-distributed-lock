package io.quarkiverse.lock.it;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.LockRegistry;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;

@Path("/locks")
@ApplicationScoped
public class LockResource {

    @Inject
    LockRegistry locks;

    @Inject
    Jobs jobs;

    @GET
    @Path("/{key}/token")
    @Blocking
    public long token(@PathParam("key") String key) {
        return locks.executeLocked(key, () -> locks.obtain(key).token().orElseThrow().fencing());
    }

    @GET
    @Path("/{key}/hold")
    @Blocking
    public String hold(@PathParam("key") String key, @QueryParam("ms") long ms) {
        return locks.executeLocked(key, () -> {
            Thread.sleep(ms);
            return "held";
        });
    }

    @GET
    @Path("/{key}/try")
    @Blocking
    public String tryLock(@PathParam("key") String key) {
        return locks.tryExecuteLocked(key, Duration.ZERO, () -> "free").orElse("busy");
    }

    @GET
    @Path("/{key}/async")
    public Uni<String> async(@PathParam("key") String key) {
        return locks.executeLockedAsync(key, () -> Uni.createFrom().item("async " + key)
                .onItem().delayIt().by(Duration.ofMillis(10)));
    }

    @GET
    @Path("/jobs/{name}")
    @Blocking
    public int job(@PathParam("name") String name, @QueryParam("ms") long ms) {
        jobs.run(name, ms);
        return jobs.runs();
    }

    @ApplicationScoped
    public static class Jobs {

        private final AtomicInteger runs = new AtomicInteger();

        @Locked(value = "job-{name}", onTimeout = OnTimeout.SKIP, registry = "jobs")
        public void run(@LockKey("name") String name, long ms) {
            runs.incrementAndGet();
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public int runs() {
            return runs.get();
        }
    }
}
