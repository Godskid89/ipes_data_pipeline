from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, field_validator
from datetime import date, datetime

class Filing(BaseModel):
    """Schema for an individual FCC filing."""
    filing_id: str
    date_received: str # ISO format YYYY-MM-DD
    docket_number: str
    submission_type: str
    filing_status: str
    document_urls: List[str] = Field(default_factory=list)
    detail_url: str

    @field_validator('document_urls')
    def validate_urls(cls, v):
        # Basic check to ensure valid structure if needed
        return v

class Company(BaseModel):
    """Schema for a structured Company entity."""
    id: str
    entity_name: str
    normalized_name: str
    entity_type: str = "Company"
    is_applicant: bool
    filing_count: int = Field(ge=0) # Must be >= 0
    filings: List[Filing]
    enrichment: dict = Field(default_factory=dict)

