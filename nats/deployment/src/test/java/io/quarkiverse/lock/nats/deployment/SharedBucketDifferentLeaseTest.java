package io.quarkiverse.lock.nats.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

class SharedBucketDifferentLeaseTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.messaging.nats.devservices.image-name", System.getProperty("nats.image", "nats:2.12-alpine"))
            .overrideConfigKey("quarkus.lock.type", "nats")
            .overrideConfigKey("quarkus.lock.a.nats.bucket", "shared")
            .overrideConfigKey("quarkus.lock.a.nats.lease", "5s")
            .overrideConfigKey("quarkus.lock.b.nats.bucket", "shared")
            .overrideConfigKey("quarkus.lock.b.nats.lease", "10s")
            .assertException(t -> {
                StringBuilder sb = new StringBuilder();
                for (Throwable c = t; c != null; c = c.getCause()) {
                    sb.append(c.getMessage()).append('\n');
                }
                assertThat(sb.toString()).contains("use one bucket per lease");
            });

    @Test
    void startupFails() {
    }
}
