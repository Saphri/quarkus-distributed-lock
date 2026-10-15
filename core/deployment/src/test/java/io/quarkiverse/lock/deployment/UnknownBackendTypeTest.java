package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.test.QuarkusExtensionTest;

class UnknownBackendTypeTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.lock.jobs.type", "redis")
            .assertException(t -> assertThat(InvalidLockedUsageTest.allMessages(t))
                    .contains("Lock registry 'jobs' uses unknown type 'redis'"));

    @Test
    void failsTheBuild() {
    }
}
