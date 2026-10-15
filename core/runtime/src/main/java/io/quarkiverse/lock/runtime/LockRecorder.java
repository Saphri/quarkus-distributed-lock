package io.quarkiverse.lock.runtime;

import java.util.Set;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class LockRecorder {

    /** Registry names referenced by the application, created at startup so misconfiguration fails fast. */
    static volatile Set<String> knownRegistries = Set.of();

    public void setKnownRegistries(Set<String> names) {
        knownRegistries = Set.copyOf(names);
    }
}
