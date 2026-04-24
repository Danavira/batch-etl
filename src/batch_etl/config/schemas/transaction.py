from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


TransactionType = Literal[
    "deposit",
    "withdrawal",
    "transfer_in",
    "transfer_out",
    "payment",
    "fee",
    "refund",
]

TransactionChannel = Literal[
    "mobile_app",
    "atm",
    "branch",
    "api",
    "card",
    "bank_transfer",
]

TransactionDirection = Literal["credit", "debit"]
TransactionStatus = Literal["pending", "completed", "failed", "reversed"]


class Transaction(BaseModel):
    id: UUID = Field(default_factory=uuid4)

    transaction_reference: str = Field(..., min_length=6, max_length=40)
    user_id: UUID

    transaction_type: TransactionType
    channel: TransactionChannel

    amount: Decimal = Field(..., max_digits=18, decimal_places=2)
    currency: str = Field(default="IDR", min_length=3, max_length=3)

    direction: TransactionDirection
    status: TransactionStatus = "pending"

    description: Optional[str] = None
    merchant_name: Optional[str] = Field(default=None, max_length=150)
    counterparty_name: Optional[str] = Field(default=None, max_length=150)
    counterparty_account_number: Optional[str] = Field(default=None, max_length=50)
    counterparty_bank_name: Optional[str] = Field(default=None, max_length=150)

    balance_before: Decimal = Field(..., max_digits=18, decimal_places=2)
    balance_after: Decimal = Field(..., max_digits=18, decimal_places=2)

    external_reference: Optional[str] = Field(default=None, max_length=100)
    processed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("amount must be greater than 0")
        return v

    @field_validator("balance_before", "balance_after")
    @classmethod
    def validate_balances(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("balance values cannot be negative")
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        return v.upper()