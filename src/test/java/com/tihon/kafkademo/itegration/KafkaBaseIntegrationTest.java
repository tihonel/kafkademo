package com.tihon.kafkademo.itegration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Durations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.io.IOException;
import java.time.Duration;

@Slf4j
@Testcontainers
public class KafkaBaseIntegrationTest {
    private static SchemaRegistryClient schemaRegistryClient;
    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withListener("kafka:19092")
                    .waitingFor(Wait.forLogMessage(".*started.*", 1));

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:latest"))
                    .withExposedPorts(8085)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("schema-registry")
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_CUB_KAFKA_MIN_BROKERS", "1")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8085")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(2));



    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        String kafkaUrl = "PLAINTEXT://" + KAFKA.getHost() + ":" + KAFKA.getMappedPort(9092);

        registry.add("spring.kafka.bootstrap-servers", () -> kafkaUrl);
        registry.add("spring.kafka.consumer.bootstrap-servers", () -> kafkaUrl);
        registry.add("spring.kafka.producer.bootstrap-servers", () -> kafkaUrl);

        registry.add("spring.kafka.consumer.properties.schema.registry.url",
                () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getFirstMappedPort());
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getFirstMappedPort());
    }

    @BeforeAll
    static void init() {
        setup("http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getFirstMappedPort());
        log.info("Registered schema");
    }

    @Test
    void shouldHaveHealthyContainers() {
        assertThat(KAFKA.isRunning()).isTrue();
        assertThat(SCHEMA_REGISTRY.isRunning()).isTrue();
    }


    static void setup(String schemaRegistryUrl) {
        await().atMost(Durations.ONE_MINUTE)
                .until(() -> KAFKA.isRunning() && SCHEMA_REGISTRY.isRunning());

        schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                100);

        registerSchema();
    }

    private static void registerSchema(){
        ParsedSchema schema = new AvroSchema(
                """
                {
                    "type":"record",
                    "name":"AvroTextDto",
                    "namespace":"com.tihon.kafkademo.dto",
                    "fields":[{
                        "name":"text",
                        "type":"string"
                }]}
                """
        );

        try {
            schemaRegistryClient.register("textMessages-value" ,schema);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Error during setup test environment", e);
        }
    }
}