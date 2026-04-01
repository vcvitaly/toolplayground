package io.github.vcvitaly.isolationplayground;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class AccountsRepository {

	private final TransactionTemplate transactionTemplate;
	private final JdbcTemplate jdbcTemplate;

	public AccountsRepository(PlatformTransactionManager transactionManager, JdbcTemplate jdbcTemplate) {
		this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jdbcTemplate = jdbcTemplate;
    }

	public void findById(int id) {
		// TODO: Implement query using transactionTemplate
	}

	public void updateAmount(int id, double amount) {
		// TODO: Implement update using transactionTemplate
	}
}
