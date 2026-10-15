package io.quarkiverse.lock.deployment;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * Announces that a lock backend type (the value of {@code quarkus.lock[."registry"].type}) is available. Produced by
 * each provider extension's deployment module, so a misconfigured type fails the build.
 */
public final class LockBackendTypeBuildItem extends MultiBuildItem {

    private final String type;

    public LockBackendTypeBuildItem(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
