package io.github.vcvitaly.isolationplayground.repeatableread;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
import io.github.vcvitaly.isolationplayground.TestUtil;
import io.github.vcvitaly.isolationplayground.TransactionalPhase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializationFailureIntererstAccrualTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(
                    statements = "INSERT INTO accounts VALUES " +
                            "(1, 'alice', 1000.00), (2, 'bob', 200.00), (3, 'bob', 800.00)",
                    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
            ),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test() {
        TransactionTemplate t1 = new TransactionTemplate(txManager);
//        t1.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        TransactionTemplate t2 = new TransactionTemplate(txManager);
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started transaction 1");
            accountsRepository.withdraw(3, 100);
            phase.set(Phases._1_TX1_WITHDRAWN_FROM_BOB);
            await(Phases._2_TX2_TRIES_UPDATE);
            TestUtil.sleep(100);
            return null;
        }));

        await(Phases._1_TX1_WITHDRAWN_FROM_BOB);
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> t2.execute(status -> {
            log.info("Started transaction 2");
            phase.set(Phases._2_TX2_TRIES_UPDATE);
            try {
                accountsRepository.runArbitrarySql("""
                        UPDATE accounts SET amount = amount * 1.01
                        WHERE client IN (
                            SELECT client
                            FROM accounts
                            GROUP BY client
                            HAVING sum(amount) >= 1000
                        );
                        """);
            } catch (Exception e) {
                log.warn("Failed to update accounts", e);
                status.setRollbackOnly();
            }
            return null;
        }));

        CompletableFuture.allOf(cf1, cf2).join();
        assertThat(accountsRepository.findAll().stream()
                .filter(a -> a.client().equals("bob"))
                .map(Account::amount).toList())
                .containsExactlyInAnyOrderElementsOf(
                        List.of(200.0, 700.0)
                );
    }

    private enum Phases implements TransactionalPhase {
        _1_TX1_WITHDRAWN_FROM_BOB,
        _2_TX2_TRIES_UPDATE
    }
}
