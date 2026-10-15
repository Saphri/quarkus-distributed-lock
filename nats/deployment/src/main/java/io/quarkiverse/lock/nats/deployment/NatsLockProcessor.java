package io.quarkiverse.lock.nats.deployment;

import io.quarkiverse.lock.deployment.LockBackendTypeBuildItem;
import io.quarkiverse.lock.nats.runtime.NatsLockBackendFactory;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class NatsLockProcessor {

    static final String FEATURE = "distributed-lock-nats";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    LockBackendTypeBuildItem natsBackend() {
        return new LockBackendTypeBuildItem(NatsLockBackendFactory.TYPE);
    }

    @BuildStep
    AdditionalBeanBuildItem beans() {
        return AdditionalBeanBuildItem.unremovableOf(NatsLockBackendFactory.class);
    }
}
