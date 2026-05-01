package io.github.vcvitaly.isolationplayground.repeatableread;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
import io.github.vcvitaly.isolationplayground.TransactionalPhase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SerializationFailureInsteadOfLostUpdateTest extends AbstractPostgresTest {

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
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> {
            t1.execute(_ -> {
                log.info("Started transaction 1");
                Account alice = accountsRepository.findById(1);
                assertThat(alice.amount()).isEqualTo(800.00);
                setPhase(Phases._1_TX1_CHECKED_BALANCE);
                await(Phases._2_TX2_CHECKED_BALANCE);
                accountsRepository.setAmount(1, alice.amount() + 100);
                return null;
            });
            setPhase(Phases._3_TX1_COMMITTED);
        }, getExecutor("tx1-thread"));

        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> t2.execute(status -> {
            log.info("Started transaction 2");
            await(Phases._1_TX1_CHECKED_BALANCE);
            Account alice = accountsRepository.findById(1);
            assertThat(alice.amount()).isEqualTo(800.00);
            setPhase(Phases._2_TX2_CHECKED_BALANCE);
            await(Phases._3_TX1_COMMITTED);
            try {
                accountsRepository.setAmount(1, alice.amount() + 100);
            } catch (Exception e) {
                log.error("Failed to set amount", e);
                status.setRollbackOnly();
            }
            return null;
        }), getExecutor("tx2-thread"));

        CompletableFuture.allOf(cf1, cf2).join();
        assertThat(accountsRepository.findById(1).amount()).isEqualTo(900);
    }

    private enum Phases implements TransactionalPhase {
        _1_TX1_CHECKED_BALANCE,
        _2_TX2_CHECKED_BALANCE,
        _3_TX1_COMMITTED
    }
}
