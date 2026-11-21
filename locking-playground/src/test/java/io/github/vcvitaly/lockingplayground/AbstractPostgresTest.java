package io.github.vcvitaly.lockingplayground;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
public abstract class AbstractPostgresTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18-alpine");

    @Autowired
    protected PlatformTransactionManager txManager;

    protected final AtomicReference<TransactionalPhase> phase = new AtomicReference<>();

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    protected abstract Logger getLog();

    protected void setPhase(TransactionalPhase phase) {
        this.phase.set(phase);
        getLog().info("set phase to : {}", phase);
    }

    protected void await(TransactionalPhase awaitedPhase) {
        Awaitility.await().until(() -> phase.get() == awaitedPhase);
    }

    protected Executor getExecutor(String name) {
        return Executors.newSingleThreadExecutor(
                r -> new Thread(r, name)
        );
    }
}
