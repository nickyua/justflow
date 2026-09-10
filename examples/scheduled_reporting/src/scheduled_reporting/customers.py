"""A bounded, synthetic customer directory and one-customer workflow actions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from justflow.resources import ConfigReader, ResourceError
from justflow.sdk import BaseAction

MAX_CUSTOMER_REFERENCE_LENGTH = 128
MAX_CUSTOMERS_PER_PAGE = 25
MAX_LOCAL_DIRECTORY_SIZE = 100
DIRECTORY_RESOURCE = "customer_directory"
DIRECTORY_KEY = "customers"


class CustomerContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class CustomerInput(CustomerContract):
    customer_ref: str = Field(
        min_length=1, max_length=MAX_CUSTOMER_REFERENCE_LENGTH, pattern=r"^[a-zA-Z0-9_-]+$"
    )


class CustomerCheck(CustomerInput):
    state: Literal["checked"] = "checked"


class CustomerWorkflowInput(CustomerContract):
    customer: CustomerInput
    input: CustomerInput | None = None


class PageRequest(CustomerContract):
    cursor: int = Field(ge=0, le=MAX_LOCAL_DIRECTORY_SIZE)
    page_size: int = Field(ge=1, le=MAX_CUSTOMERS_PER_PAGE)


class CustomerPage(CustomerContract):
    customers: tuple[CustomerInput, ...] = Field(max_length=MAX_CUSTOMERS_PER_PAGE)
    next_cursor: int | None


class SummaryRequest(CustomerContract):
    outcomes: tuple[dict[str, JsonValue], ...] = Field(max_length=MAX_CUSTOMERS_PER_PAGE)
    next_cursor: int | None


class BatchSummary(SummaryRequest):
    attempted: int = Field(ge=0, le=MAX_CUSTOMERS_PER_PAGE)


DIRECTORY_ADAPTER = TypeAdapter(tuple[CustomerInput, ...])


class LocalCustomerService(BaseAction):
    async def fetch_page(self, input: object) -> dict[str, object]:
        request = PageRequest.model_validate(input)
        resource = self.resources[DIRECTORY_RESOURCE]
        if not isinstance(resource, ConfigReader):
            raise ResourceError("Customer directory is unavailable")
        customers = DIRECTORY_ADAPTER.validate_python(resource.get(DIRECTORY_KEY))
        if len(customers) > MAX_LOCAL_DIRECTORY_SIZE:
            raise ResourceError("Synthetic customer directory exceeds its local bound")
        end = request.cursor + request.page_size
        page = CustomerPage(
            customers=customers[request.cursor : end],
            next_cursor=end if end < len(customers) else None,
        )
        return page.model_dump(mode="json")

    async def check_customer(self, input: object) -> dict[str, object]:
        request = CustomerInput.model_validate(input)
        return CustomerCheck(customer_ref=request.customer_ref).model_dump(mode="json")

    async def summarize(self, input: object) -> dict[str, object]:
        request = SummaryRequest.model_validate(input)
        return BatchSummary(**request.model_dump(), attempted=len(request.outcomes)).model_dump(
            mode="json"
        )
