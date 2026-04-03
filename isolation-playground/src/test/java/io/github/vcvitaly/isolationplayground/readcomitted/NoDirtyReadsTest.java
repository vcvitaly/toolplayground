package io.github.vcvitaly.isolationplayground.readcomitted;

import io.github.vcvitaly.isolationplayground.AbstractPostgresTest;
import io.github.vcvitaly.isolationplayground.Account;
import io.github.vcvitaly.isolationplayground.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class NoDirtyReadsTest extends AbstractPostgresTest {

    @Test
    @SqlGroup({
            @Sql(scripts = "/sql/initial_accounts.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD),
            @Sql(scripts = "/sql/truncate_accounts.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    })
    void test() {
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
}
