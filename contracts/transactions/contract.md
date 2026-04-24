# Contract: transactions

**Owner:** batch-etl
**Version:** 1.0.0
**Status:** Active
**Enforced by:** `src/batch_etl/config/schemas/transaction.py`

## Guarantees

- Every record has a unique `id` (UUID).
- `amount` is always positive and in the smallest currency subunit — **not** a major unit (e.g. 15000 means IDR 15,000, not IDR 15,000,000).
- `currency` is always uppercased ISO 4217. Default is `IDR`.
- `balance_before` and `balance_after` are never negative.
- `created_at` is always in UTC.
- `status` at insert time may be `pending` — downstream consumers must not assume `completed`.

## Schema

| Field | Type | Required | Constraints | Semantics |
|---|---|---|---|---|
| `id` | UUID | yes | unique | Primary key, generated at source |
| `transaction_reference` | string | yes | 6–40 chars | Business-level reference number, not the internal `id` |
| `user_id` | UUID | yes | — | Foreign key to `users.id` |
| `transaction_type` | enum | yes | see below | The nature of the transaction |
| `channel` | enum | yes | see below | How the transaction was initiated |
| `amount` | decimal | yes | > 0, 18 digits max, 2 decimal places | Transaction amount in `currency`, always positive regardless of direction |
| `currency` | string | yes | 3 chars, ISO 4217 | Currency of `amount`. Default: `IDR` |
| `direction` | enum | yes | `credit` \| `debit` | Whether money moved into (`credit`) or out of (`debit`) the account |
| `status` | enum | yes | see below | Processing state at time of record creation |
| `description` | string | no | — | Free-text description, unstructured |
| `merchant_name` | string | no | max 150 chars | Populated for `payment` and `card` transactions |
| `counterparty_name` | string | no | max 150 chars | Name of the other party in a transfer |
| `counterparty_account_number` | string | no | max 50 chars | — |
| `counterparty_bank_name` | string | no | max 150 chars | — |
| `balance_before` | decimal | yes | >= 0, 18 digits max, 2 decimal places | Account balance immediately before this transaction |
| `balance_after` | decimal | yes | >= 0, 18 digits max, 2 decimal places | Account balance immediately after this transaction |
| `external_reference` | string | no | max 100 chars | Reference from an external system (e.g. payment gateway) |
| `processed_at` | datetime | no | UTC | When the transaction was processed. Null if still `pending` |
| `created_at` | datetime | yes | UTC | When the record was created at source |
| `updated_at` | datetime | yes | UTC | When the record was last updated at source |

## Enums

**`transaction_type`**
`deposit` | `withdrawal` | `transfer_in` | `transfer_out` | `payment` | `fee` | `refund`

**`channel`**
`mobile_app` | `atm` | `branch` | `api` | `card` | `bank_transfer`

**`direction`**
`credit` | `debit`

**`status`**
`pending` | `completed` | `failed` | `reversed`

## Known limitations

- `description` is unstructured and should not be parsed programmatically.
- `counterparty_*` fields are only meaningful for `transfer_in` and `transfer_out` types; they may be null for all others.
- `processed_at` is null for `pending` records and may be backfilled when status transitions to `completed`.
