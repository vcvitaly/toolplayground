package io.github.vcvitaly.isolationplayground.repeatableread;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
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

public class NoNonRepeatableReadsTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(scripts = "/sql/initial_accounts.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD),
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
            await(Phases._1_TX2_STARTED);
            accountsRepository.addClient(4, "charlie", 100.0);
            phase.set(Phases._2_TX1_INSERTED_NEW_ACCOUNT);
            await(Phases._3_TX2_CHECKED_FIRST_TIME);
            return null;
        })).whenComplete((v, e) -> {
            if (e != null) {
                log.error("Error occurred in TX1", e);
            }
            phase.set(Phases._4_TX1_COMMITTED);
        });

        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> t2.execute(_ -> {
            log.info("Started transaction 2");
            phase.set(Phases._1_TX2_STARTED);
            await(Phases._2_TX1_INSERTED_NEW_ACCOUNT);
            assertThat(accountsRepository.findAll().stream().map(Account::client).toList())
                    .containsExactlyInAnyOrderElementsOf(
                            List.of("alice", "bob", "bob")
                    );
            phase.set(Phases._3_TX2_CHECKED_FIRST_TIME);
            await(Phases._4_TX1_COMMITTED);
            assertThat(accountsRepository.findAll().stream().map(Account::client).toList())
                    .containsExactlyInAnyOrderElementsOf(
                            List.of("alice", "bob", "bob")
                    );
            return null;
        }));

        CompletableFuture.allOf(cf1, cf2).join();
        assertThat(accountsRepository.findAll().stream().map(Account::client).toList())
                .containsExactlyInAnyOrderElementsOf(
                        List.of("alice", "bob", "bob", "charlie")
                );
    }

    private enum Phases implements TransactionalPhase {
        _1_TX2_STARTED,
        _2_TX1_INSERTED_NEW_ACCOUNT,
        _3_TX2_CHECKED_FIRST_TIME,
        _4_TX1_COMMITTED
    }
}
