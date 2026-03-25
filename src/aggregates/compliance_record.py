# src/aggregates/compliance_record.py
"""
Compliance Record Aggregate - Tracks regulatory checks and compliance verdicts.
This aggregate is separate from LoanApplication to maintain regulatory independence.
"""

from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import logging

from src.aggregates.base import Aggregate
from src.event_store import EventStore
from src.models.events import (
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    StoredEvent, BaseEvent
)
from src.models.errors import DomainError, PreconditionFailedError

logger = logging.getLogger(__name__)


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
    - Cannot issue compliance clearance without all mandatory checks
    - Every check must reference the specific regulation version
    - Compliance record must be append-only - no modifications
    - Overrides require justification and approver
    - Must maintain chain of custody for audit purposes
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
        self.override_justification: Optional[str] = None
        self.audit_chain: List[str] = []
        self.initiated_at: Optional[datetime] = None
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        """Load aggregate from event store"""
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(application_id)
        for event in events:
            agg.apply_event(event)
        
        return agg
    
    # =========================================================================
    # Event Handlers
    # =========================================================================
    
    def _on_ComplianceCheckInitiated(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance check initiation"""
        required_checks = set(payload.get("required_checks", []))
        self.required_checks.update(required_checks)
        self.regulation_versions.add(payload.get("regulation_version", "unknown"))
        self.initiated_at = datetime.utcnow()
        self.status = ComplianceStatus.IN_PROGRESS
        
        for check in required_checks:
            self.checks_initiated.add(check)
        
        logger.info(f"Compliance checks initiated for {self.application_id}: {len(required_checks)} checks")
    
    def _on_ComplianceRulePassed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance rule passed"""
        rule_id = payload["rule_id"]
        self.checks_passed.add(rule_id)
        self.regulation_versions.add(payload.get("regulation_version", "unknown"))
        
        # Add to audit chain
        self.audit_chain.append(f"PASSED:{rule_id}:{datetime.utcnow().isoformat()}")
        
        # Update status
        if self.required_checks and self.checks_passed == self.required_checks:
            self.status = ComplianceStatus.PASSED
            self.completed_at = datetime.utcnow()
            logger.info(f"All compliance checks passed for {self.application_id}")
        elif self.checks_passed:
            self.status = ComplianceStatus.PARTIAL
        
        logger.debug(f"Compliance rule passed for {self.application_id}: {rule_id}")
    
    def _on_ComplianceRuleFailed(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance rule failed"""
        self.checks_failed.append({
            "rule_id": payload["rule_id"],
            "reason": payload.get("failure_reason"),
            "regulation_version": payload.get("regulation_version"),
            "timestamp": datetime.utcnow().isoformat(),
            "details": payload.get("details", {})
        })
        self.regulation_versions.add(payload.get("regulation_version", "unknown"))
        
        # Add to audit chain
        self.audit_chain.append(f"FAILED:{payload['rule_id']}:{payload.get('failure_reason', 'unknown')}")
        
        self.status = ComplianceStatus.FAILED
        self.completed_at = datetime.utcnow()
        
        logger.warning(f"Compliance rule failed for {self.application_id}: {payload['rule_id']} - {payload.get('failure_reason')}")
    
    def _on_ComplianceOverride(self, payload: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Handle compliance override"""
        self.status = ComplianceStatus.OVERRIDDEN
        self.overridden_by = payload.get("overridden_by")
        self.override_reason = payload.get("reason")
        self.override_justification = payload.get("justification")
        self.completed_at = datetime.utcnow()
        
        # Add to audit chain
        self.audit_chain.append(f"OVERRIDDEN:{payload['overridden_by']}:{payload.get('reason')}")
        
        logger.info(f"Compliance overridden for {self.application_id} by {payload['overridden_by']}")
    
    # =========================================================================
    # Business Rule Validation Methods
    # =========================================================================
    
    def assert_compliance_clearance(self) -> None:
        """
        Rule #5: Cannot issue compliance clearance without all mandatory checks
        This is the critical enforcement point for compliance dependency.
        """
        if not self.required_checks:
            raise PreconditionFailedError(
                f"Compliance checks not initiated for application {self.application_id}",
                suggested_action="Call initiate_compliance_checks first"
            )
        
        missing_checks = self.required_checks - self.checks_passed
        
        if missing_checks:
            raise PreconditionFailedError(
                f"Cannot issue compliance clearance. Missing required checks: {missing_checks}",
                missing_checks=list(missing_checks),
                required_checks=list(self.required_checks),
                passed_checks=list(self.checks_passed),
                suggested_action="Complete all required compliance checks before approval"
            )
        
        if self.checks_failed:
            raise PreconditionFailedError(
                f"Cannot issue compliance clearance. Failed checks: {[c['rule_id'] for c in self.checks_failed]}",
                failed_checks=self.checks_failed,
                suggested_action="Resolve compliance failures or request override"
            )
    
    def assert_rule_exists(self, rule_id: str) -> None:
        """Validate that rule ID is in required checks"""
        if rule_id not in self.required_checks and self.required_checks:
            raise DomainError(
                f"Rule {rule_id} not in required checks for application {self.application_id}",
                rule_id=rule_id,
                required_checks=list(self.required_checks),
                suggested_action="Check rule ID or initiate compliance checks with correct rules"
            )
    
    def assert_no_duplicate_check(self, rule_id: str) -> None:
        """Prevent duplicate check submissions"""
        if rule_id in self.checks_passed:
            raise DomainError(
                f"Rule {rule_id} already passed for application {self.application_id}",
                rule_id=rule_id,
                existing_status="passed",
                suggested_action="Cannot submit duplicate check"
            )
        
        for failed in self.checks_failed:
            if failed["rule_id"] == rule_id:
                raise DomainError(
                    f"Rule {rule_id} already failed for application {self.application_id}",
                    rule_id=rule_id,
                    existing_status="failed",
                    failure_reason=failed.get("reason"),
                    suggested_action="Cannot resubmit failed check"
                )
    
    def assert_override_allowed(self, user_role: str) -> None:
        """Validate override permissions"""
        allowed_roles = ["compliance_officer", "admin", "regulatory_auditor"]
        
        if user_role not in allowed_roles:
            raise DomainError(
                f"User {user_role} not authorized to override compliance decisions",
                user_role=user_role,
                allowed_roles=allowed_roles,
                suggested_action="Request override from compliance officer"
            )
    
    def assert_check_not_initiated(self) -> None:
        """Validate that compliance checks haven't been initiated yet"""
        if self.required_checks:
            raise DomainError(
                f"Compliance checks already initiated for application {self.application_id}",
                initiated_at=self.initiated_at.isoformat() if self.initiated_at else None,
                required_checks=list(self.required_checks),
                suggested_action="Continue with existing checks"
            )
    
    # =========================================================================
    # Event Creation Methods
    # =========================================================================
    
    def create_check_initiated_event(
        self,
        required_checks: List[str],
        regulation_version: str,
        initiated_by: str
    ) -> ComplianceCheckInitiated:
        """Create compliance check initiation event"""
        return ComplianceCheckInitiated(
            application_id=self.application_id,
            check_id=f"init-{self.application_id}-{self.version + 1}",
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
            check_id=f"{rule_id}-{self.application_id}-{len(self.checks_passed) + 1}",
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
            check_id=f"{rule_id}-{self.application_id}-{len(self.checks_failed) + 1}",
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
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def get_compliance_summary(self) -> Dict[str, Any]:
        """Get compliance summary for reporting"""
        return {
            "application_id": self.application_id,
            "status": self.status,
            "required_checks": list(self.required_checks),
            "checks_passed": list(self.checks_passed),
            "checks_failed": self.checks_failed,
            "regulation_versions": list(self.regulation_versions),
            "initiated_at": self.initiated_at.isoformat() if self.initiated_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "overridden_by": self.overridden_by,
            "override_reason": self.override_reason,
            "audit_chain_length": len(self.audit_chain)
        }
    
    def is_compliant(self) -> bool:
        """Check if application is compliant"""
        return (self.status == ComplianceStatus.PASSED or 
                self.status == ComplianceStatus.OVERRIDDEN)
    
    def get_failed_checks(self) -> List[Dict[str, Any]]:
        """Get list of failed checks"""
        return self.checks_failed