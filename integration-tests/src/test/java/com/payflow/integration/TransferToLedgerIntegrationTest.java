package com.payflow.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Full-flow integration test: login -> create accounts -> POST transfer -> Kafka -> ledger entry.
 *
 * <p>Everything is real: Postgres, Kafka (KRaft) and Redis run as Testcontainers, and the three
 * services are built from their own Dockerfiles and run as containers on a shared network, so no
 * docker-compose step is needed. Auth (Phase 1) is exercised for real: the test logs in via
 * {@code POST /auth/login} and sends the returned JWT on every protected call.
 */
class TransferToLedgerIntegrationTest {

    private static final String JWT_SECRET =
            "integration-test-secret-must-be-at-least-32-chars-long";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;
    private static GenericContainer<?> redis;
    private static GenericContainer<?> accounts;
    private static GenericContainer<?> transactions;
    private static GenericContainer<?> ledger;

    @BeforeAll
    static void startAll() {
        network = Network.newNetwork();

        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
                .withNetwork(network).withNetworkAliases("postgres")
                .withUsername("payflow").withPassword("payflow")
                .withDatabaseName("postgres")
                // Creates accounts_db, transactions_db and ledger_db, same as docker-compose.
                .withCopyFileToContainer(
                        MountableFile.forHostPath(repoRoot().resolve("init-db.sql")),
                        "/docker-entrypoint-initdb.d/init-db.sql");

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                .withNetwork(network).withNetworkAliases("kafka");

        redis = new GenericContainer<>(DockerImageName.parse("redis:7"))
                .withNetwork(network).withNetworkAliases("redis")
                .withExposedPorts(6379);

        postgres.start();
        kafka.start();
        redis.start();

        // Testcontainers' KafkaContainer exposes its in-network BROKER listener as <alias>:9093.
        accounts = app("accounts-service", 8081)
                .withEnv("DB_URL", "jdbc:postgresql://postgres:5432/accounts_db")
                .withEnv("REDIS_HOST", "redis");
        accounts.start();

        transactions = app("transactions-service", 8082)
                .withEnv("DB_URL", "jdbc:postgresql://postgres:5432/transactions_db")
                .withEnv("KAFKA_BROKERS", "kafka:9093")
                .withEnv("ACCOUNTS_URL", "http://accounts-service:8081");
        transactions.start();

        ledger = app("ledger-service", 8083)
                .withEnv("DB_URL", "jdbc:postgresql://postgres:5432/ledger_db")
                .withEnv("KAFKA_BROKERS", "kafka:9093")
                .withEnv("REDIS_HOST", "redis")
                .withEnv("GEMINI_API_KEY", "");
        ledger.start();
    }

    @AfterAll
    static void stopAll() {
        for (GenericContainer<?> c : new GenericContainer<?>[] {ledger, transactions, accounts, redis}) {
            if (c != null) c.stop();
        }
        if (kafka != null) kafka.stop();
        if (postgres != null) postgres.stop();
        if (network != null) network.close();
    }

    @Test
    void transferFlowsThroughKafkaIntoAValidHashChainedLedgerEntry() throws Exception {
        String accountsUrl = base(accounts, 8081);
        String txUrl = base(transactions, 8082);
        String ledgerUrl = base(ledger, 8083);

        // Auth really is live: unauthenticated access is rejected.
        assertThat(send("GET", ledgerUrl + "/ledger", null, null).statusCode()).isEqualTo(401);

        // Auth setup: log in through the real endpoint and reuse the JWT on every protected call.
        HttpResponse<String> login = send("POST", txUrl + "/auth/login",
                "{\"username\":\"admin\",\"password\":\"admin123\"}", null);
        assertThat(login.statusCode()).isEqualTo(200);
        String token = JSON.readTree(login.body()).get("token").asText();

        String from = createAccount(accountsUrl, token, "Alice", "1000.00");
        String to = createAccount(accountsUrl, token, "Bob", "50.00");

        HttpResponse<String> transfer = send("POST", txUrl + "/transactions",
                "{\"fromAccount\":\"" + from + "\",\"toAccount\":\"" + to
                        + "\",\"amount\":250.00,\"description\":\"integration test\","
                        + "\"merchant\":\"TestMart\"}", token);
        assertThat(transfer.statusCode()).isEqualTo(200);
        JsonNode txn = JSON.readTree(transfer.body());
        assertThat(txn.get("status").asText()).isEqualTo("COMPLETED");
        String txnId = txn.get("id").asText();

        // Poll (bounded) until the Kafka consumer has written the ledger entry.
        JsonNode[] found = new JsonNode[1];
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().untilAsserted(() -> {
                    HttpResponse<String> r = send("GET", ledgerUrl + "/ledger", null, token);
                    assertThat(r.statusCode()).isEqualTo(200);
                    found[0] = null;
                    for (JsonNode e : JSON.readTree(r.body())) {
                        if (txnId.equals(e.get("transactionId").asText())) {
                            found[0] = e;
                        }
                    }
                    assertThat(found[0]).as("ledger entry for transaction " + txnId).isNotNull();
                });

        JsonNode entry = found[0];
        assertThat(entry.get("fromAccount").asText()).isEqualTo(from);
        assertThat(entry.get("toAccount").asText()).isEqualTo(to);
        assertThat(entry.get("amount").decimalValue()).isEqualByComparingTo("250.00");
        assertThat(entry.get("entryHash").asText()).matches("[0-9a-f]{64}");
        assertThat(entry.get("chainIndex").isNumber()).isTrue();

        // Hash validity: the ledger re-derives every hash and walks the chain itself.
        // (/ledger/verify is intentionally unauthenticated.)
        JsonNode verify = JSON.readTree(send("GET", ledgerUrl + "/ledger/verify", null, null).body());
        assertThat(verify.get("valid").asBoolean()).as(verify.get("message").asText()).isTrue();
        assertThat(verify.get("entriesChecked").asLong()).isGreaterThanOrEqualTo(1);

        // Balances: 1000 - 250 and 50 + 250.
        assertThat(balance(accountsUrl, token, from)).isEqualByComparingTo("750.00");
        assertThat(balance(accountsUrl, token, to)).isEqualByComparingTo("300.00");
    }

    // ---- helpers ----------------------------------------------------------------------------

    private static GenericContainer<?> app(String module, int port) {
        Path dir = repoRoot().resolve(module);
        ImageFromDockerfile image = new ImageFromDockerfile("ledgergrid-it-" + module, false)
                .withFileFromPath(".", dir);
        return new GenericContainer<>(image)
                .withNetwork(network).withNetworkAliases(module)
                .withEnv("JWT_SECRET", JWT_SECRET)
                .withExposedPorts(port)
                .waitingFor(Wait.forHttp("/actuator/health").forPort(port).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    private static Path repoRoot() {
        // Maven runs tests with the module dir as cwd; the repo root is its parent.
        return Paths.get("").toAbsolutePath().getParent();
    }

    private static String base(GenericContainer<?> c, int port) {
        return "http://" + c.getHost() + ":" + c.getMappedPort(port);
    }

    private static String createAccount(String accountsUrl, String token, String owner, String balance)
            throws Exception {
        HttpResponse<String> r = send("POST", accountsUrl + "/accounts",
                "{\"ownerName\":\"" + owner + "\",\"initialBalance\":" + balance
                        + ",\"currency\":\"INR\"}", token);
        assertThat(r.statusCode()).isEqualTo(201);
        return JSON.readTree(r.body()).get("id").asText();
    }

    private static BigDecimal balance(String accountsUrl, String token, String id) throws Exception {
        HttpResponse<String> r = send("GET", accountsUrl + "/accounts/" + id, null, token);
        assertThat(r.statusCode()).isEqualTo(200);
        return JSON.readTree(r.body()).get("balance").decimalValue();
    }

    private static HttpResponse<String> send(String method, String url, String body, String token)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json");
        if (token != null) b.header("Authorization", "Bearer " + token);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }
}
