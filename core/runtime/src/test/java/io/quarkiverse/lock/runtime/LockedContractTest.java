package io.quarkiverse.lock.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkiverse.lock.OnTimeout;
import io.quarkiverse.lock.runtime.LockedContract.Dispatch;

class LockedContractTest {

    @Test
    void parsesPlaceholders() {
        LockedContract.KeyTemplate t = LockedContract.parse("invoice-{id}:{part}");
        assertThat(t.literals()).containsExactly("invoice-", ":", "");
        assertThat(t.placeholders()).containsExactly("id", "part");
        assertThat(LockedContract.parse("plain").literals()).containsExactly("plain");
    }

    @Test
    void rejectsMalformedKeys() {
        assertThatThrownBy(() -> LockedContract.parse("a-{id")).hasMessageContaining("unbalanced braces");
        assertThatThrownBy(() -> LockedContract.parse("a-}")).hasMessageContaining("unbalanced '}'");
        assertThatThrownBy(() -> LockedContract.parse("{id}}")).hasMessageContaining("unbalanced '}'");
        assertThatThrownBy(() -> LockedContract.parse("a-{}")).hasMessageContaining("invalid placeholder {}");
        assertThatThrownBy(() -> LockedContract.parse("{a{b}")).hasMessageContaining("invalid placeholder {a{b}");
    }

    @Test
    void validatesKeyAgainstLockKeyParameters() {
        assertThat(LockedContract.validateKey("invoice-{id}", names(null, "id"), false)).isEmpty();
        assertThat(LockedContract.validateKey("invoice-{idd}", names("id"), false)).containsExactlyInAnyOrder(
                "lock key 'invoice-{idd}' references {idd} but no parameter is annotated @LockKey(\"idd\")",
                "@LockKey(\"id\") is not used in lock key 'invoice-{idd}'");
        assertThat(LockedContract.validateKey("{id}", names("id", "id"), false))
                .containsExactly("duplicate @LockKey(\"id\")");
        assertThat(LockedContract.validateKey("k", names("a}"), false)).contains("invalid @LockKey name 'a}'");
        assertThat(LockedContract.validateKey(" ", names(), false)).containsExactly("the lock key must not be empty");
        assertThat(LockedContract.validateKey(" ", names("id"), false)).containsExactlyInAnyOrder(
                "the lock key must not be empty", "@LockKey(\"id\") is not used in lock key ' '");
        assertThat(LockedContract.validateKey("{x", names("x"), false))
                .containsExactly("unbalanced braces in lock key '{x'");
    }

    @Test
    void classLevelKeysHaveNoPlaceholders() {
        assertThat(LockedContract.validateKey("jobs", names(), true)).isEmpty();
        assertThat(LockedContract.validateKey("jobs-{id}", names(), true))
                .containsExactly("class-level lock keys cannot contain placeholders; annotate the methods instead");
    }

    @Test
    void bindsAndResolvesKeys() {
        LockedContract.BoundKey key = LockedContract.bind("order-{id}/{line}", names("line", null, "id"));
        assertThat(key.resolve(new Object[] { 7, "ignored", 42L })).isEqualTo("order-42/7");
        assertThat(key.resolve(new Object[] { null, "ignored", 42L })).isEqualTo("order-42/null");
        assertThat(LockedContract.bind("static", names("unused")).resolve(new Object[] { 1 })).isEqualTo("static");
        assertThatThrownBy(() -> LockedContract.bind("order-{id}", names()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("references {id}");
    }

    @Test
    void classifiesReturnTypes() {
        assertThat(LockedContract.classify("void")).isEqualTo(Dispatch.SYNC);
        assertThat(LockedContract.classify("java.lang.String")).isEqualTo(Dispatch.SYNC);
        assertThat(LockedContract.classify("io.smallrye.mutiny.Uni")).isEqualTo(Dispatch.UNI);
        assertThat(LockedContract.classify("java.util.concurrent.CompletionStage")).isEqualTo(Dispatch.COMPLETION_STAGE);
        assertThat(LockedContract.classify("java.util.concurrent.CompletableFuture"))
                .isEqualTo(Dispatch.COMPLETION_STAGE);
        assertThatThrownBy(() -> LockedContract.classify("io.smallrye.mutiny.Multi"))
                .hasMessageContaining("streams (Multi) are not supported");
        assertThatThrownBy(() -> LockedContract.classify("java.util.concurrent.Flow$Publisher"))
                .hasMessageContaining("streams (Flow$Publisher) are not supported");
    }

    @Test
    void skipRequiresAVoidResult() {
        assertThat(LockedContract.validateReturnType("void", null, OnTimeout.SKIP)).isEmpty();
        assertThat(LockedContract.validateReturnType("io.smallrye.mutiny.Uni", "java.lang.Void", OnTimeout.SKIP))
                .isEmpty();
        assertThat(LockedContract.validateReturnType("java.util.concurrent.CompletionStage", "java.lang.Void",
                OnTimeout.SKIP)).isEmpty();
        assertThat(LockedContract.validateReturnType("java.lang.String", null, OnTimeout.SKIP))
                .singleElement().asString().startsWith("onTimeout = SKIP requires a void");
        assertThat(LockedContract.validateReturnType("io.smallrye.mutiny.Uni", "java.lang.String", OnTimeout.SKIP))
                .singleElement().asString().endsWith("found io.smallrye.mutiny.Uni<java.lang.String>");
        assertThat(LockedContract.validateReturnType("io.smallrye.mutiny.Uni", null, OnTimeout.SKIP)).hasSize(1);
        assertThat(LockedContract.validateReturnType("java.lang.String", null, OnTimeout.THROW)).isEmpty();
        assertThat(LockedContract.validateReturnType("io.smallrye.mutiny.Multi", "java.lang.String", OnTimeout.THROW))
                .singleElement().asString().contains("streams (Multi)");
    }

    @Test
    void parsesWait() {
        assertThat(LockedContract.parseWait("")).isNull();
        assertThat(LockedContract.parseWait(null)).isNull();
        assertThat(LockedContract.parseWait("5s")).isEqualTo(Duration.ofSeconds(5));
        assertThat(LockedContract.parseWait("PT0S")).isEqualTo(Duration.ZERO);
        assertThatThrownBy(() -> LockedContract.parseWait("soon")).hasMessage("invalid waitTimeout 'soon'");
        assertThatThrownBy(() -> LockedContract.parseWait("-5s")).hasMessage("waitTimeout must not be negative");
    }

    private static List<String> names(String... names) {
        return Arrays.asList(names);
    }
}
