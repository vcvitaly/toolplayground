package io.github.vcvitaly.lockingplayground.onlineauction;

import io.github.vcvitaly.lockingplayground.AbstractPostgresTest;
import io.github.vcvitaly.lockingplayground.TransactionalPhase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class UpdateAuctionFirstTest extends AbstractPostgresTest {

    private final AuctionService auctionService;

    @Autowired
    public UpdateAuctionFirstTest(JdbcTemplate jdbcTemplate) {
        auctionService = new AuctionService(jdbcTemplate);
    }

    @Test
    void test() {
        auctionService.createTablesAndInitAuction();

        TransactionTemplate t1 = new TransactionTemplate(txManager);
        TransactionTemplate t2 = new TransactionTemplate(txManager);

        int auctionId = 1;
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> t1.execute(_ -> {
            log.info("Started transaction 1");
            int tx1Price = 10;
            assertThat(auctionService.updateAuction(auctionId, tx1Price)).isOne();
            setPhase(Phases._1_TX1_UPDATED_AUCTION);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertThat(auctionService.createBid(auctionId, tx1Price)).isOne();
            return null;
        }), getExecutor("tx1-thread"));

        AtomicLong tx2Start = new AtomicLong();
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> {
            await(Phases._1_TX1_UPDATED_AUCTION);
            tx2Start.set(System.currentTimeMillis());
            t2.execute(_ -> {
                log.info("Started transaction 2");
                int tx2Price = 20;
                assertThat(auctionService.updateAuction(auctionId, tx2Price)).isOne();
                assertThat(auctionService.createBid(auctionId, tx2Price)).isOne();
                return null;
            });
        }, getExecutor("tx2-thread")).whenComplete((v, e) -> {
            log.info("Transaction 2 completed");
            IO.println("TX2 took %d ms".formatted(System.currentTimeMillis() - tx2Start.get()));
        });

        CompletableFuture.allOf(cf1, cf2).join();
        assertThat(auctionService.getAuctionPrice(auctionId)).isEqualTo(20);

        auctionService.deleteTables();
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    private enum Phases implements TransactionalPhase {
        _1_TX1_UPDATED_AUCTION
    }
}
