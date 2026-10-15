package io.quarkiverse.lock.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.lock.LockKey;
import io.quarkiverse.lock.Locked;
import io.quarkiverse.lock.OnTimeout;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Multi;

class InvalidLockedUsageTest {

    @RegisterExtension
    static final QuarkusExtensionTest TEST = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Broken.class))
            .assertException(t -> {
                String msg = allMessages(t);
                assertThat(msg).contains("references {idd} but no parameter is annotated @LockKey(\"idd\")");
                assertThat(msg).contains("@LockKey(\"id\") is not used");
                assertThat(msg).contains("invalid waitTimeout 'soon'");
                assertThat(msg).contains("onTimeout = SKIP requires a void");
                assertThat(msg).contains("streams (Multi) are not supported");
            });

    static String allMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            sb.append(t.getMessage()).append('\n');
            for (Throwable s : t.getSuppressed()) {
                sb.append(allMessages(s));
            }
            t = t.getCause();
        }
        return sb.toString();
    }

    @ApplicationScoped
    public static class Broken {

        @Locked("invoice-{idd}")
        public void typo(@LockKey("id") long id) {
        }

        @Locked(value = "k", waitTimeout = "soon")
        public void badDuration() {
        }

        @Locked(value = "k", onTimeout = OnTimeout.SKIP)
        public String skipWithResult() {
            return "x";
        }

        @Locked("k")
        public Multi<String> stream() {
            return Multi.createFrom().empty();
        }
    }

    @Test
    void failsTheBuild() {
        // the assertion is in assertException
    }
}
