# Contracts

Each subdirectory is a contract for one data entity. A contract is the authoritative agreement between the producer (data generator) and the consumer (ETL pipeline and downstream analytics).

## What a contract contains

- **Schema** — field names, types, nullability
- **Value rules** — constraints on what values are valid
- **Semantics** — what fields actually mean (not derivable from the type alone)
- **Guarantees** — freshness, completeness, SLA
- **Ownership and versioning** — who owns it, what version it is, change policy

## Contracts

| Entity | Contract |
|---|---|
| Transactions | [contracts/transactions/contract.md](transactions/contract.md) |
| Users | [contracts/users/contract.md](users/contract.md) |

## Enforcement

The Pydantic models in `src/batch_etl/config/schemas/` are the runtime enforcement of these contracts. If a contract changes, the schema model must change with it — and vice versa, a schema model change is a contract change and must be reflected here.

## Change policy

- **Backward-compatible changes** (adding a nullable field, widening a constraint) — update the contract, bump the minor version, notify consumers
- **Breaking changes** (removing a field, narrowing a constraint, changing a type) — requires a new contract version and explicit consumer sign-off before merging
