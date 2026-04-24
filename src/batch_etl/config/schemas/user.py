from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, EmailStr, Field, field_validator


AccountType = Literal["savings", "current", "business"]
KYCStatus = Literal["pending", "verified", "rejected"]
AccountStatus = Literal["active", "frozen", "suspended", "closed"]


class User(BaseModel):
    id: UUID = Field(default_factory=uuid4)

    customer_number: str = Field(..., min_length=3, max_length=20)
    full_name: str = Field(..., min_length=2, max_length=150)
    email: EmailStr
    phone_number: str = Field(..., min_length=8, max_length=20)

    password_hash: str
    date_of_birth: date

    government_id_type: Optional[str] = Field(default=None, max_length=30)
    government_id_number: Optional[str] = Field(default=None, max_length=100)

    address_line_1: Optional[str] = Field(default=None, max_length=255)
    address_line_2: Optional[str] = Field(default=None, max_length=255)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default=None, max_length=100)
    postal_code: Optional[str] = Field(default=None, max_length=20)
    country: str = Field(default="Japan", max_length=100)

    account_number: str = Field(..., min_length=5, max_length=30)
    account_type: AccountType = "savings"
    currency: str = Field(default="JPY", min_length=3, max_length=3)
    balance: Decimal = Field(default=Decimal("0.00"), max_digits=18, decimal_places=2)

    kyc_status: KYCStatus = "pending"
    account_status: AccountStatus = "active"

    last_login_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("balance")
    @classmethod
    def validate_balance(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("balance cannot be negative")
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        return v.upper()