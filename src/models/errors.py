# src/models/errors.py
"""
Custom exception types for The Ledger.
"""

from typing import Optional, Any


class LedgerError(Exception):
    """Base exception for all Ledger errors."""
    pass


class OptimisticConcurrencyError(LedgerError):
    """Raised when expected_version doesn't match current stream version."""
    
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
        self.message = message or (
            f"Optimistic concurrency conflict on stream {stream_id}. "
            f"Expected version {expected_version}, actual version {actual_version}"
        )
        super().__init__(self.message)


class DomainError(LedgerError):
    """Raised when business rules are violated."""
    
    def __init__(self, message: str, details: Optional[dict] = None):
        self.details = details or {}
        super().__init__(message)


class StreamNotFoundError(LedgerError):
    """Raised when trying to load a stream that doesn't exist."""
    
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream {stream_id} not found")


class PreconditionFailedError(LedgerError):
    """Raised when a precondition for an operation is not met."""
    
    def __init__(self, precondition: str, details: Optional[dict] = None):
        self.precondition = precondition
        self.details = details or {}
        super().__init__(f"Precondition failed: {precondition}")


class InvalidEventError(LedgerError):
    """Raised when an event doesn't meet validation requirements."""
    
    def __init__(self, event_type: str, reason: str):
        self.event_type = event_type
        self.reason = reason
        super().__init__(f"Invalid event {event_type}: {reason}")


class ProjectionError(LedgerError):
    """Raised when projection processing fails."""
    
    def __init__(self, projection_name: str, event_id: str, reason: str):
        self.projection_name = projection_name
        self.event_id = event_id
        super().__init__(f"Projection {projection_name} failed on event {event_id}: {reason}")


class IntegrityError(LedgerError):
    """Raised when integrity check fails."""
    
    def __init__(self, message: str, tampered_events: list):
        self.tampered_events = tampered_events
        super().__init__(message)