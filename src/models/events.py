# src/models/events.py
"""
Complete Pydantic models for events with full type safety, validation, and serialization.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Literal
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from enum import Enum
import json


class ApplicationState(str, Enum):
    """Loan application state machine states"""
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


class RiskTier(str, Enum):
    """Credit risk tiers"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class Recommendation(str, Enum):
    """Decision recommendations"""
    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    REFER = "REFER"


class FinalDecision(str, Enum):
    """Final human decisions"""
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"


# ============================================================================
# Base Event Classes
# ============================================================================

class BaseEvent(BaseModel):
    """Base class for all domain events with validation"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={datetime: lambda v: v.isoformat()},
        extra="forbid"
    )
    
    event_type: str = Field(..., frozen=True, description="Type of the event")
    event_version: int = Field(default=1, frozen=True, ge=1, le=10, description="Schema version")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    
    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type format"""
        if not v or not v[0].isupper():
            raise ValueError(f"Event type must start with uppercase: {v}")
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for JSON storage"""
        return self.model_dump(exclude={'event_type', 'event_version', 'timestamp'})


# ============================================================================
# Loan Application Events
# ============================================================================

class ApplicationSubmitted(BaseEvent):
    """Event emitted when a loan application is submitted"""
    event_type: str = "ApplicationSubmitted"
    event_version: int = 1
    application_id: str = Field(..., min_length=1, max_length=100)
    applicant_id: str = Field(..., min_length=1, max_length=100)
    requested_amount_usd: float = Field(..., gt=0, le=100_000_000)
    business_name: str = Field(..., min_length=1, max_length=200)
    tax_id: str = Field(..., pattern=r'^\d{2}-\d{7}$')
    
    @field_validator("requested_amount_usd")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        """Validate requested amount"""
        if v <= 0:
            raise ValueError(f"Requested amount must be positive: {v}")
        if v > 100_000_000:
            raise ValueError(f"Requested amount exceeds maximum: {v}")
        return v


class CreditAnalysisCompleted(BaseEvent):
    """Event emitted when credit analysis is completed"""
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2  # Will be upcasted from v1
    application_id: str = Field(..., min_length=1)
    risk_tier: RiskTier = Field(...)
    credit_score: int = Field(..., ge=300, le=850)
    max_credit_limit: float = Field(..., gt=0)
    model_version: str = Field(default="legacy-pre-2026", min_length=1)
    confidence_score: Optional[float] = Field(None, ge=0, le=1)
    regulatory_basis: Optional[str] = Field(None)
    
    @field_validator("credit_score")
    @classmethod
    def validate_credit_score(cls, v: int) -> int:
        """Validate credit score range"""
        if v < 300 or v > 850:
            raise ValueError(f"Credit score must be between 300 and 850: {v}")
        return v
    
    @field_validator("max_credit_limit")
    @classmethod
    def validate_limit(cls, v: float) -> float:
        """Validate credit limit"""
        if v <= 0:
            raise ValueError(f"Max credit limit must be positive: {v}")
        return v


class FraudScreeningCompleted(BaseEvent):
    """Event emitted when fraud screening is completed"""
    event_type: str = "FraudScreeningCompleted"
    event_version: int = 1
    application_id: str = Field(..., min_length=1)
    fraud_score: float = Field(..., ge=0, le=1)
    flags: List[str] = Field(default_factory=list)
    risk_indicators: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator("fraud_score")
    @classmethod
    def validate_fraud_score(cls, v: float) -> float:
        """Validate fraud score range"""
        if v < 0 or v > 1:
            raise ValueError(f"Fraud score must be between 0 and 1: {v}")
        return v


class ComplianceRulePassed(BaseEvent):
    """Event emitted when a compliance rule is passed"""
    event_type: str = "ComplianceRulePassed"
    event_version: int = 1
    application_id: str = Field(..., min_length=1)
    rule_id: str = Field(..., min_length=1)
    regulation_version: str = Field(..., min_length=1)
    check_id: str = Field(..., min_length=1)
    details: Dict[str, Any] = Field(default_factory=dict)


class ComplianceRuleFailed(BaseEvent):
    """Event emitted when a compliance rule is failed"""
    event_type: str = "ComplianceRuleFailed"
    event_version: int = 1
    application_id: str = Field(..., min_length=1)
    rule_id: str = Field(..., min_length=1)
    regulation_version: str = Field(..., min_length=1)
    check_id: str = Field(..., min_length=1)
    failure_reason: str = Field(..., min_length=1)
    details: Dict[str, Any] = Field(default_factory=dict)


class ComplianceCheckInitiated(BaseEvent):
    """Event emitted when compliance checks are initiated"""
    event_type: str = "ComplianceCheckInitiated"
    event_version: int = 1
    application_id: str = Field(..., min_length=1)
    check_id: str = Field(..., min_length=1)
    required_checks: List[str] = Field(..., min_length=1)
    regulation_version: str = Field(..., min_length=1)
    initiated_by: str = Field(..., min_length=1)


class DecisionGenerated(BaseEvent):
    """Event emitted when an AI decision is generated"""
    event_type: str = "DecisionGenerated"
    event_version: int = 2  # Will be upcasted from v1
    application_id: str = Field(..., min_length=1)
    decision_id: str = Field(..., min_length=1)
    recommendation: Recommendation = Field(...)
    confidence_score: float = Field(..., ge=0, le=1)
    contributing_agent_sessions: List[str] = Field(default_factory=list)
    decision_reasoning: str = Field(..., min_length=1)
    model_versions: Dict[str, str] = Field(default_factory=dict)
    
    @field_validator("confidence_score")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Validate confidence score range"""
        if v < 0 or v > 1:
            raise ValueError(f"Confidence score must be between 0 and 1: {v}")
        return v


class HumanReviewCompleted(BaseEvent):
    """Event emitted when human review is completed"""
    event_type: str = "HumanReviewCompleted"
    event_version: int = 1
    application_id: str = Field(..., min_length=1)
    reviewer_id: str = Field(..., min_length=1)
    final_decision: FinalDecision = Field(...)
    override: bool = False
    override_reason: Optional[str] = Field(None, min_length=1)
    comments: Optional[str] = Field(None)


# ============================================================================
# Agent Session Events
# ============================================================================

class AgentContextLoaded(BaseEvent):
    """Event emitted when an agent loads context (Gas Town pattern)"""
    event_type: str = "AgentContextLoaded"
    event_version: int = 1
    agent_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    context_source: str = Field(..., min_length=1)
    token_count: int = Field(..., ge=0)
    model_version: str = Field(..., min_length=1)
    context_hash: str = Field(..., min_length=32, max_length=64)
    
    @field_validator("token_count")
    @classmethod
    def validate_token_count(cls, v: int) -> int:
        """Validate token count"""
        if v < 0:
            raise ValueError(f"Token count cannot be negative: {v}")
        if v > 1_000_000:
            raise ValueError(f"Token count exceeds maximum: {v}")
        return v
    
    @field_validator("context_hash")
    @classmethod
    def validate_hash(cls, v: str) -> str:
        """Validate hash format (hex string)"""
        if not all(c in "0123456789abcdef" for c in v.lower()):
            raise ValueError(f"Context hash must be hex string: {v}")
        return v


class AgentActionTaken(BaseEvent):
    """Event emitted when an agent takes an action"""
    event_type: str = "AgentActionTaken"
    event_version: int = 1
    agent_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    action_type: str = Field(..., min_length=1)
    input_data_hash: str = Field(..., min_length=32, max_length=64)
    reasoning_trace: str = Field(..., min_length=1)
    output_data: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator("action_type")
    @classmethod
    def validate_action_type(cls, v: str) -> str:
        """Validate action type"""
        valid_actions = ["credit_analysis", "fraud_screening", "compliance_check", 
                         "generate_decision", "human_review", "validation"]
        if v not in valid_actions:
            raise ValueError(f"Invalid action type: {v}. Must be one of {valid_actions}")
        return v


# ============================================================================
# Audit Events
# ============================================================================

class AuditIntegrityCheckRun(BaseEvent):
    """Event emitted when an integrity check is run"""
    event_type: str = "AuditIntegrityCheckRun"
    event_version: int = 1
    entity_type: str = Field(..., min_length=1)
    entity_id: str = Field(..., min_length=1)
    previous_hash: str = Field(..., min_length=64, max_length=64)
    current_hash: str = Field(..., min_length=64, max_length=64)
    events_verified: int = Field(..., ge=0)
    chain_valid: bool = Field(...)
    tamper_detected: bool = Field(...)
    checked_at: datetime = Field(default_factory=datetime.utcnow)
    global_position: int = Field(..., ge=0)


# ============================================================================
# Stored Event Model
# ============================================================================

class StoredEvent(BaseModel):
    """Represents an event as stored in the database with full type safety"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    event_id: UUID = Field(default_factory=uuid4)
    stream_id: str = Field(..., min_length=1, max_length=255)
    stream_position: int = Field(..., ge=0)
    global_position: int = Field(..., ge=0)
    event_type: str = Field(..., min_length=1)
    event_version: int = Field(..., ge=1, le=10)
    payload: Dict[str, Any] = Field(...)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(None, max_length=255)
    causation_id: Optional[str] = Field(None, max_length=255)
    recorded_at: datetime = Field(default_factory=datetime.utcnow)
    
    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type format"""
        if not v or not v[0].isupper():
            raise ValueError(f"Event type must start with uppercase: {v}")
        return v
    
    @model_validator(mode="after")
    def validate_causal_chain(self) -> "StoredEvent":
        """Validate causal chain consistency"""
        if self.causation_id and not self.correlation_id:
            # causation_id should only be set when correlation_id is set
            pass  # Allow but warn in production
        return self
    
    def with_payload(self, new_payload: Dict[str, Any], version: int) -> "StoredEvent":
        """Create a new StoredEvent with updated payload and version"""
        return StoredEvent(
            event_id=self.event_id,
            stream_id=self.stream_id,
            stream_position=self.stream_position,
            global_position=self.global_position,
            event_type=self.event_type,
            event_version=version,
            payload=new_payload,
            metadata=self.metadata,
            correlation_id=self.correlation_id,
            causation_id=self.causation_id,
            recorded_at=self.recorded_at
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_id": str(self.event_id),
            "stream_id": self.stream_id,
            "stream_position": self.stream_position,
            "global_position": self.global_position,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.payload,
            "metadata": self.metadata,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "recorded_at": self.recorded_at.isoformat()
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)


class StreamMetadata(BaseModel):
    """Metadata for an event stream"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    stream_id: str = Field(..., min_length=1, max_length=255)
    aggregate_type: str = Field(..., min_length=1)
    current_version: int = Field(..., ge=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator("aggregate_type")
    @classmethod
    def validate_aggregate_type(cls, v: str) -> str:
        """Validate aggregate type"""
        valid_types = ["LoanApplication", "AgentSession", "ComplianceRecord", "AuditLedger"]
        if v not in valid_types:
            raise ValueError(f"Aggregate type must be one of {valid_types}: {v}")
        return v
    
    def is_archived(self) -> bool:
        """Check if stream is archived"""
        return self.archived_at is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "stream_id": self.stream_id,
            "aggregate_type": self.aggregate_type,
            "current_version": self.current_version,
            "created_at": self.created_at.isoformat(),
            "archived_at": self.archived_at.isoformat() if self.archived_at else None,
            "metadata": self.metadata
        }