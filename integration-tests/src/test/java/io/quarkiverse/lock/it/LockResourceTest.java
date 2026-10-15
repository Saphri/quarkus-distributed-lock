package io.quarkiverse.lock.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.time.Duration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class LockResourceTest {

    static void awaitBusy(String key, String registry) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() -> "busy".equals(
                given().queryParam("registry", registry == null ? "" : registry).get("/locks/" + key + "/try").asString()));
    }

    @Test
    void fencingTokensIncrease() {
        long first = Long.parseLong(given().get("/locks/fence/token").then().statusCode(200).extract().asString());
        long second = Long.parseLong(given().get("/locks/fence/token").then().statusCode(200).extract().asString());
        assertThat(second).isGreaterThan(first);
    }

    @Test
    void heldLockIsBusy() throws Exception {
        CompletableFuture<String> holder = CompletableFuture
                .supplyAsync(() -> given().queryParam("ms", 3000).get("/locks/busy/hold").then().extract().asString());
        awaitBusy("busy", null);
        assertThat(holder.get(10, TimeUnit.SECONDS)).isEqualTo("held");
        given().get("/locks/busy/try").then().statusCode(200).body(is("free"));
    }

    @Test
    void async() {
        given().get("/locks/a1/async").then().statusCode(200).body(is("async a1"));
    }

    @Test
    void skipWhenJobRuns() throws Exception {
        int before = Integer.parseInt(given().queryParam("ms", 0).get("/locks/jobs/warmup").then().extract().asString());
        CompletableFuture<String> running = CompletableFuture
                .supplyAsync(() -> given().queryParam("ms", 3000).get("/locks/jobs/nightly").then().extract().asString());
        awaitBusy("job-nightly", "jobs");
        // skipped: the count only moves by the running job
        given().queryParam("ms", 0).get("/locks/jobs/nightly").then().statusCode(200);
        running.get(10, TimeUnit.SECONDS);
        int after = Integer.parseInt(given().queryParam("ms", 0).get("/locks/jobs/other").then().extract().asString());
        assertThat(after - before).isEqualTo(2);
    }
}
