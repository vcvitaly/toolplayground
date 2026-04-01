# AGENTS.md

## Project Overview
Spring Boot playground for exploring distributed systems concepts — transaction isolation levels, Kafka messaging, and more. Primary artifact is **integration tests**; production code exists only to support them.

## Tech Stack
- **Java 21**, Spring Boot 3.x
- **Spring Data JDBC** — no JPA, no Hibernate
- **Testcontainers** — all infrastructure (Postgres, Kafka, …) spun up per test suite
- **JUnit 5**

## Non-Negotiable Constraints

### Database
- Spring Data JDBC only — no `spring-data-jpa`, no `@Entity`, no `EntityManager`
- Schema managed via `schema.sql` (Spring Boot auto-runs on startup)

### Transactions
- Use `TransactionTemplate` for all programmatic transaction control
- **Never** use `@Transactional` annotation

### Infrastructure
- Every external dependency (DB, broker, …) is started by Testcontainers
- No assumption of a locally running service; tests must be self-contained

## Testing Conventions
- One `@SpringBootTest` application context per thematic package, shared via `@TestConfiguration` + Testcontainers `@Container static`
- Test class names end with `IntegrationTest`
- Each test must be independent and leave no side-effects (use `TransactionTemplate` rollback or `@Sql` cleanup)

## Code Style
- Prefer explicit, readable test setup over clever abstractions
- No Lombok
- Record types for DTOs/result holders where appropriate