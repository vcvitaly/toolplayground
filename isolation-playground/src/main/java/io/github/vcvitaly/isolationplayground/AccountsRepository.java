package io.github.vcvitaly.isolationplayground;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

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

	public void withdraw(int id, double amount) {
		updateAmount(id, amount, Operation.WITHDRAW);
	}

	public void deposit(int id, double amount) {
		updateAmount(id, amount, Operation.DEPOSIT);
	}

	public void setAmount(int id, double amount) {
		jdbcTemplate.update(
				"UPDATE accounts SET amount = :amount WHERE id = :id",
				Map.of("id", id, "amount", amount)
		);
		log.info("Set amount for account {} to {}", id, amount);
	}

	private void updateAmount(int id, double amount, Operation operation) {
		jdbcTemplate.update(
				"UPDATE accounts SET amount = amount %s :amount WHERE id = :id".formatted(operation.getSymbol()),
				Map.of("id", id, "amount", amount)
		);
		log.info("Updated amount for account {} to {}", id, amount);
	}

	@RequiredArgsConstructor
	@Getter
	private enum Operation {
		DEPOSIT("+"), WITHDRAW("-");

		private final String symbol;
	}
}
