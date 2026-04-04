package io.github.vcvitaly.isolationplayground.readcomitted;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadSkewTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(scripts = "/sql/initial_accounts.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void test() {
        AtomicReference<Phases> phase = new AtomicReference<>();
        TransactionTemplate t1 = new TransactionTemplate(txManager);
        t1.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        TransactionTemplate t2 = new TransactionTemplate(txManager);
        t2.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started thread 1");
            accountsRepository.withdraw(2, 100);
            phase.set(Phases._1_WITHDRAWN_FROM_BOB1);
            Awaitility.await().until(() -> phase.get() == Phases._2_CHECKED_BOB1_AMOUNT);
            accountsRepository.deposit(3, 100);
            phase.set(Phases._3_DEPOSITED_TO_BOB2);
            return null;
        }));

        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
            log.info("Started thread 2");
            Awaitility.await().until(() -> phase.get() == Phases._1_WITHDRAWN_FROM_BOB1);
            t2.execute(_ -> {
                Account bob2 = accountsRepository.findById(2);
                assertThat(bob2.amount()).isEqualTo(100);
                phase.set(Phases._2_CHECKED_BOB1_AMOUNT);
                Awaitility.await().until(() -> phase.get() == Phases._3_DEPOSITED_TO_BOB2);
                Account bob3 = accountsRepository.findById(3);
                assertThat(bob3.amount()).isEqualTo(1000);
                assertThat(bob2.amount() + bob3.amount()).isEqualTo(1100);
                return null;
            });
        });

        CompletableFuture.allOf(cf1, cf2).join();
    }

    private enum Phases {
        _1_WITHDRAWN_FROM_BOB1,
        _2_CHECKED_BOB1_AMOUNT,
        _3_DEPOSITED_TO_BOB2
    }
}
