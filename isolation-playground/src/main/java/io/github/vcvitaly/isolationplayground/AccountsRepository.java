package io.github.vcvitaly.isolationplayground;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import lombok.extern.slf4j.Slf4j;

@Repository
public class AccountsRepository {

	private static final Logger log = LoggerFactory.getLogger(AccountsRepository.class);

	private final NamedParameterJdbcTemplate jdbcTemplate;

	public AccountsRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

	public Account findById(int id) {
		return jdbcTemplate.queryForObject(
				"SELECT id, client, amount FROM accounts WHERE id = :id",
				Map.of("id", id),
				(rs, rowNum) -> new Account(rs.getInt("id"), rs.getString("client"), rs.getDouble("amount"))
		);
	}

	public void updateAmount(int id, double amount) {
		jdbcTemplate.update(
				"UPDATE accounts SET amount = :amount WHERE id = :id",
				Map.of("id", id, "amount", amount)
		);
		log.info("Updated amount for account {} to {}", id, amount);
	}
}
