# src/models/errors.py
"""
Structured error types for the event store with suggested actions for LLM consumption.
"""

from typing import Optional, Any, Dict, List
from enum import Enum


class ErrorSeverity(str, Enum):
    """Error severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorCategory(str, Enum):
    """Error categories"""
    VALIDATION = "VALIDATION"
    CONCURRENCY = "CONCURRENCY"
    DOMAIN = "DOMAIN"
    PRECONDITION = "PRECONDITION"
    INTEGRITY = "INTEGRITY"
    SYSTEM = "SYSTEM"


class EventStoreError(Exception):
    """Base exception for event store errors with structured output"""
    
    error_type: str = "EventStoreError"
    error_category: ErrorCategory = ErrorCategory.SYSTEM
    severity: ErrorSeverity = ErrorSeverity.ERROR
    
    def __init__(self, message: str, **kwargs):
        self.message = message
        self.details = kwargs
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for structured error responses"""
        return {
            "error_type": self.error_type,
            "error_category": self.error_category.value,
            "severity": self.severity.value,
            "message": self.message,
            "suggested_action": self.details.get("suggested_action"),
            **{k: v for k, v in self.details.items() if k != "suggested_action"}
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        import json
        return json.dumps(self.to_dict())


class OptimisticConcurrencyError(EventStoreError):
    """Raised when expected version doesn't match current stream version"""
    
    error_type = "OptimisticConcurrencyError"
    error_category = ErrorCategory.CONCURRENCY
    severity = ErrorSeverity.WARNING
    
    def __init__(
        self, 
        stream_id: str, 
        expected_version: int, 
        actual_version: int,
        message: Optional[str] = None
    ):
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        msg = message or f"Concurrency conflict on stream {stream_id}: expected version {expected_version}, actual version {actual_version}"
        super().__init__(
            msg,
            stream_id=stream_id,
            expected_version=expected_version,
            actual_version=actual_version,
            suggested_action="reload_stream_and_retry",
            retry_strategy={
                "max_retries": 3,
                "base_delay_ms": 100,
                "backoff_factor": 2,
                "jitter_ms": 10,
                "expected_success_rate": 99.5
            }
        )


class DomainError(EventStoreError):
    """Raised when domain rules are violated"""
    
    error_type = "DomainError"
    error_category = ErrorCategory.DOMAIN
    severity = ErrorSeverity.ERROR
    
    def __init__(self, message: str, rule_id: Optional[str] = None, **kwargs):
        super().__init__(
            message,
            rule_id=rule_id,
            **kwargs
        )


class PreconditionFailedError(EventStoreError):
    """Raised when preconditions for an operation are not met"""
    
    error_type = "PreconditionFailedError"
    error_category = ErrorCategory.PRECONDITION
    severity = ErrorSeverity.WARNING
    
    def __init__(
        self, 
        message: str, 
        missing_prerequisite: Optional[str] = None,
        required_tool: Optional[str] = None,
        example_call: Optional[Dict] = None,
        **kwargs
    ):
        super().__init__(
            message,
            missing_prerequisite=missing_prerequisite,
            required_tool=required_tool,
            example_call=example_call,
            **kwargs
        )


class StreamNotFoundError(EventStoreError):
    """Raised when a stream doesn't exist"""
    
    error_type = "StreamNotFoundError"
    error_category = ErrorCategory.VALIDATION
    severity = ErrorSeverity.INFO
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(
            f"Stream not found: {stream_id}",
            stream_id=stream_id,
            suggested_action="create_stream_first",
            alternative="Use expected_version=-1 to create new stream"
        )


class InvalidStateTransitionError(DomainError):
    """Raised when an invalid state transition is attempted"""
    
    error_type = "InvalidStateTransitionError"
    
    def __init__(
        self, 
        message: str, 
        current_state: str, 
        attempted_state: str, 
        valid_transitions: List[str],
        **kwargs
    ):
        super().__init__(
            message,
            current_state=current_state,
            attempted_state=attempted_state,
            valid_transitions=valid_transitions,
            suggested_action=f"Transition to one of: {valid_transitions}",
            **kwargs
        )


class IntegrityCheckFailedError(EventStoreError):
    """Raised when integrity check fails"""
    
    error_type = "IntegrityCheckFailedError"
    error_category = ErrorCategory.INTEGRITY
    severity = ErrorSeverity.CRITICAL
    
    def __init__(
        self,
        message: str,
        entity_type: str,
        entity_id: str,
        broken_at_position: int,
        expected_hash: str,
        actual_hash: str,
        **kwargs
    ):
        super().__init__(
            message,
            entity_type=entity_type,
            entity_id=entity_id,
            broken_at_position=broken_at_position,
            expected_hash=expected_hash[:16] + "...",
            actual_hash=actual_hash[:16] + "...",
            suggested_action="restore_from_backup_and_investigate",
            severity="CRITICAL",
            **kwargs
        )


class RateLimitExceededError(EventStoreError):
    """Raised when rate limit is exceeded"""
    
    error_type = "RateLimitExceededError"
    error_category = ErrorCategory.SYSTEM
    severity = ErrorSeverity.WARNING
    
    def __init__(
        self,
        resource: str,
        limit: int,
        period_seconds: int,
        retry_after_seconds: int,
        **kwargs
    ):
        super().__init__(
            f"Rate limit exceeded for {resource}: {limit} requests per {period_seconds} seconds",
            resource=resource,
            limit=limit,
            period_seconds=period_seconds,
            retry_after_seconds=retry_after_seconds,
            suggested_action=f"wait_{retry_after_seconds}_seconds",
            **kwargs
        )