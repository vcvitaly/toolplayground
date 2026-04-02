package io.github.vcvitaly.isolationplayground;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReadCommittedIntegrationTest {

    private final Logger log = LoggerFactory.getLogger(ReadCommittedIntegrationTest.class);

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Autowired
    private AccountsRepository accountsRepository;

    @Autowired
    private PlatformTransactionManager txManager;

    @Test
    @SqlGroup({
            @Sql(scripts = "/sql/initial_accounts.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    void noDirtyReads() {
        TransactionTemplate t1 = new TransactionTemplate(txManager);
        t1.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        TransactionTemplate t2 = new TransactionTemplate(txManager);
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started thread 1");
            accountsRepository.withdraw(1, 200);
            Account alice = accountsRepository.findById(1);
            assertThat(alice.amount()).isEqualTo(800);
            TestUtil.sleep(2_000);
            return null;
        }));

        TestUtil.sleep(5);

        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
            log.info("Started thread 2");
            TestUtil.sleep(100);
            log.info("Waited for 100 ms and going to check the balance for 1");
            t2.execute(_ -> {
                Account alice = accountsRepository.findById(1);
                assertThat(alice.amount()).isEqualTo(1000);
                return null;
            });
        });

        CompletableFuture.allOf(cf1, cf2).join();

        Account alice = accountsRepository.findById(1);
        assertThat(alice.amount()).isEqualTo(800);
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }
}
