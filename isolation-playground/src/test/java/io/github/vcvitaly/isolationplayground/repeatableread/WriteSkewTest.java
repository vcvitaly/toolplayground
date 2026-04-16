package io.github.vcvitaly.isolationplayground.repeatableread;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.TransactionalPhase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteSkewTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(
                    statements = "INSERT INTO accounts VALUES " +
                            "(2, 'bob', 200.00), (3, 'bob', 700.00)",
                    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
            ),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test() {
        TransactionTemplate t1 = new TransactionTemplate(txManager);
        t1.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        TransactionTemplate t2 = new TransactionTemplate(txManager);
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started transaction 1");
            Double sum = accountsRepository.runArbitratrySelect("SELECT sum(amount) FROM accounts WHERE client = 'bob'", Double.class);
            assertThat(sum).isEqualTo(900);
            phase.set(Phases._1_TX1_CHECKED_BALANCE);
            await(Phases._2_TX2_CHECKED_BALANCE);
            accountsRepository.withdraw(2, 600.0);
            phase.set(Phases._3_TX1_UPDATED_BALANCE);
            await(Phases._4_TX2_COMMITTED);
            return null;
        }), getExecutor("tx1-thread"));

        await(Phases._1_TX1_CHECKED_BALANCE);
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
            t2.execute(status -> {
                log.info("Started transaction 2");
                Double sum = accountsRepository.runArbitratrySelect("SELECT sum(amount) FROM accounts WHERE client = 'bob'", Double.class);
                assertThat(sum).isEqualTo(900);
                phase.set(Phases._2_TX2_CHECKED_BALANCE);
                await(Phases._3_TX1_UPDATED_BALANCE);
                accountsRepository.withdraw(3, 600.0);
                return null;
            });
        }, getExecutor("tx2-thread")).whenComplete((v, e) -> {
            log.info("Transaction 2 completed");
            phase.set(Phases._4_TX2_COMMITTED);
        });

        CompletableFuture.allOf(cf1, cf2).join();
        Double sum = accountsRepository.runArbitratrySelect("SELECT sum(amount) FROM accounts WHERE client = 'bob'", Double.class);
        assertThat(sum).isEqualTo(-300.0);
    }

    private enum Phases implements TransactionalPhase {
        _1_TX1_CHECKED_BALANCE,
        _2_TX2_CHECKED_BALANCE,
        _3_TX1_UPDATED_BALANCE,
        _4_TX2_COMMITTED
    }
}
