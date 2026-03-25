# src/aggregates/compliance_record.py
"""
Compliance Record Aggregate - Tracks regulatory checks and compliance verdicts.
This aggregate is separate from LoanApplication to maintain regulatory independence.
"""

from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from src.aggregates.base import Aggregate
from src.event_store import EventStore
from src.models.events import (
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    StoredEvent, BaseEvent
)
from src.models.errors import DomainError, PreconditionFailedError


class ComplianceStatus:
    """Compliance status enum"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    PARTIAL = "PARTIAL"
    PASSED = "PASSED"
    FAILED = "FAILED"
    OVERRIDDEN = "OVERRIDDEN"


class ComplianceRecordAggregate(Aggregate):
    """
    Compliance Record Aggregate - Manages all regulatory checks for an application.
    
    Business Rules:
    1. Cannot issue compliance clearance without all mandatory checks
    2. Every check must reference the specific regulation version
    3. Compliance record must be append-only - no modifications
    4. Overrides require justification and approver
    5. Must maintain chain of custody for audit purposes
    """
    
    def __init__(self, application_id: str):
        super().__init__()
        self.application_id = application_id
        self.status = ComplianceStatus.PENDING
        self.checks_passed: Set[str] = set()
        self.checks_failed: List[Dict[str, Any]] = []
        self.checks_initiated: Set[str] = set()
        self.required_checks: Set[str] = set()
        self.regulation_versions: Set[str] = set()
        self.completed_at: Optional[datetime] = None
        self.overridden_by: Optional[str] = None
        self.override_reason: Optional[str] = None
        self.audit_chain: List[str] = []  # Hash chain for integrity
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        """Load aggregate from event store"""
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(application_id)
        for event in events:
            agg.apply_event(event)
        
        return agg
    
    def _on_ComplianceCheckInitiated(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance check initiation"""
        required_checks = set(payload.get("required_checks", []))
        self.required_checks.update(required_checks)
        self.status = ComplianceStatus.IN_PROGRESS
        
        for check in required_checks:
            self.checks_initiated.add(check)
    
    def _on_ComplianceRulePassed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance rule passed"""
        rule_id = payload["rule_id"]
        self.checks_passed.add(rule_id)
        self.regulation_versions.add(payload.get("regulation_version", "unknown"))
        
        # Update status
        if self.required_checks and self.checks_passed == self.required_checks:
            self.status = ComplianceStatus.PASSED
            self.completed_at = datetime.utcnow()
        elif self.checks_passed:
            self.status = ComplianceStatus.PARTIAL
    
    def _on_ComplianceRuleFailed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance rule failed"""
        self.checks_failed.append({
            "rule_id": payload["rule_id"],
            "reason": payload.get("failure_reason"),
            "regulation_version": payload.get("regulation_version"),
            "timestamp": datetime.utcnow().isoformat()
        })
        self.status = ComplianceStatus.FAILED
        self.completed_at = datetime.utcnow()
    
    def _on_ComplianceOverride(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance override"""
        self.status = ComplianceStatus.OVERRIDDEN
        self.overridden_by = payload.get("overridden_by")
        self.override_reason = payload.get("reason")
        self.completed_at = datetime.utcnow()
    
    # Business Rule Validation Methods
    
    def assert_compliance_clearance(self) -> None:
        """Business Rule #5: Cannot issue compliance clearance without all mandatory checks"""
        if self.required_checks and self.checks_passed != self.required_checks:
            missing = self.required_checks - self.checks_passed
            raise PreconditionFailedError(
                f"Cannot issue compliance clearance. Missing required checks: {missing}",
                missing_checks=list(missing),
                required_checks=list(self.required_checks),
                passed_checks=list(self.checks_passed)
            )
        
        if self.checks_failed:
            raise PreconditionFailedError(
                f"Cannot issue compliance clearance. Failed checks: {[c['rule_id'] for c in self.checks_failed]}",
                failed_checks=self.checks_failed
            )
    
    def assert_rule_exists(self, rule_id: str) -> None:
        """Validate that rule ID is in required checks"""
        if rule_id not in self.required_checks and self.required_checks:
            raise DomainError(f"Rule {rule_id} not in required checks for this application")
    
    def assert_no_duplicate_check(self, rule_id: str) -> None:
        """Prevent duplicate check submissions"""
        if rule_id in self.checks_passed:
            raise DomainError(f"Rule {rule_id} already passed")
        
        for failed in self.checks_failed:
            if failed["rule_id"] == rule_id:
                raise DomainError(f"Rule {rule_id} already failed")
    
    def assert_override_allowed(self, user_role: str) -> None:
        """Validate override permissions"""
        if user_role != "compliance_officer" and user_role != "admin":
            raise DomainError(f"User {user_role} not authorized to override compliance decisions")
    
    # Event Creation Methods
    
    def create_check_initiated_event(
        self,
        required_checks: List[str],
        regulation_version: str,
        initiated_by: str
    ) -> ComplianceCheckInitiated:
        """Create compliance check initiation event"""
        return ComplianceCheckInitiated(
            application_id=self.application_id,
            check_id=f"check-{self.application_id}-{self.version + 1}",
            required_checks=required_checks,
            regulation_version=regulation_version,
            initiated_by=initiated_by
        )
    
    def create_rule_passed_event(
        self,
        rule_id: str,
        regulation_version: str,
        details: Dict[str, Any]
    ) -> ComplianceRulePassed:
        """Create compliance rule passed event"""
        return ComplianceRulePassed(
            application_id=self.application_id,
            rule_id=rule_id,
            regulation_version=regulation_version,
            check_id=f"{rule_id}-{self.application_id}",
            details=details
        )
    
    def create_rule_failed_event(
        self,
        rule_id: str,
        regulation_version: str,
        failure_reason: str,
        details: Dict[str, Any]
    ) -> ComplianceRuleFailed:
        """Create compliance rule failed event"""
        return ComplianceRuleFailed(
            application_id=self.application_id,
            rule_id=rule_id,
            regulation_version=regulation_version,
            check_id=f"{rule_id}-{self.application_id}",
            failure_reason=failure_reason,
            details=details
        )
    
    def create_override_event(
        self,
        overridden_by: str,
        reason: str,
        justification: str
    ) -> BaseEvent:
        """Create compliance override event"""
        from src.models.events import BaseEvent
        class ComplianceOverride(BaseEvent):
            event_type: str = "ComplianceOverride"
            event_version: int = 1
            application_id: str
            overridden_by: str
            reason: str
            justification: str
        
        return ComplianceOverride(
            application_id=self.application_id,
            overridden_by=overridden_by,
            reason=reason,
            justification=justification
        )
    
    def get_compliance_summary(self) -> Dict[str, Any]:
        """Get compliance summary for reporting"""
        return {
            "application_id": self.application_id,
            "status": self.status,
            "required_checks": list(self.required_checks),
            "checks_passed": list(self.checks_passed),
            "checks_failed": self.checks_failed,
            "regulation_versions": list(self.regulation_versions),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "overridden_by": self.overridden_by,
            "override_reason": self.override_reason
        }