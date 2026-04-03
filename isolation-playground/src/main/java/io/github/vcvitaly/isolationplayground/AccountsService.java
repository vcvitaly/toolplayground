package io.github.vcvitaly.isolationplayground;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AccountsService {

	private final AccountsRepository accountsRepository;

	public Account findById(int id) {
		return accountsRepository.findById(id);
	}

	public void withdraw(int id, double amount) {
		accountsRepository.withdraw(id, amount);
	}

	public void deposit(int id, double amount) {
		accountsRepository.deposit(id, amount);
	}

	public void readAndDeposit(int id, double amount) {
		final Account acc = accountsRepository.findById(id);
		accountsRepository.deposit(id, acc.amount() + amount);
		log.info("Read and deposited {} to account {}", amount, id);
	}
}
