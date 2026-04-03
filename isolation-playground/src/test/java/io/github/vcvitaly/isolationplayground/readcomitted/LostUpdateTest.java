package io.github.vcvitaly.isolationplayground.readcomitted;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
import io.github.vcvitaly.isolationplayground.TestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class LostUpdateTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(
                    statements = "INSERT INTO accounts VALUES (1, 'alice', 800.00)",
                    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
            ),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test() {
        TransactionTemplate t1 = new TransactionTemplate(txManager);
        t1.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        TransactionTemplate t2 = new TransactionTemplate(txManager);
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started transaction 1");
            deposit100ForAlice();
            return null;
        }));

        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
            t2.execute(_ -> {
                log.info("Started transaction 2");
                deposit100ForAlice();
                return null;
            });
        });

        CompletableFuture.allOf(cf1, cf2).join();
        assertThat(accountsRepository.findById(1).amount()).isEqualTo(900);
    }

    private void deposit100ForAlice() {
        Account alice = accountsRepository.findById(1);
        TestUtil.sleep(100);
        accountsRepository.setAmount(1, alice.amount() + 100);
    }
}
