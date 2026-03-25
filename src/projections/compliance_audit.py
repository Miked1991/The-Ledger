# src/projections/compliance_audit.py
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
from src.projections.base import AsyncProjection
from src.models.events import StoredEvent

class ComplianceAuditProjection(AsyncProjection):
    """
    Compliance audit projection with temporal query support.
    Maintains snapshots for efficient point-in-time queries.
    """
    
    def __init__(self, snapshot_interval: int = 100):
        """
        Args:
            snapshot_interval: Number of events between snapshots
        """
        super().__init__("compliance_audit")
        self.snapshot_interval = snapshot_interval
        self._application_states: Dict[str, Dict[str, Any]] = {}
        self._event_counters: Dict[str, int] = defaultdict(int)
    
    async def handle_batch(self, events: List[StoredEvent]) -> None:
        """Handle a batch of events and create snapshots"""
        for event in events:
            await self._handle_event(event)
            
            # Create snapshot at interval
            app_id = self._get_application_id(event)
            if app_id:
                self._event_counters[app_id] += 1
                if self._event_counters[app_id] % self.snapshot_interval == 0:
                    await self._create_snapshot(app_id, event.global_position)
    
    async def _handle_event(self, event: StoredEvent) -> None:
        """Handle a single compliance-related event"""
        app_id = self._get_application_id(event)
        if not app_id:
            return
        
        # Initialize state if needed
        if app_id not in self._application_states:
            self._application_states[app_id] = {
                "application_id": app_id,
                "submitted_at": None,
                "checks_passed": [],
                "checks_failed": [],
                "compliance_status": "PENDING",
                "regulation_versions": set(),
                "last_event_position": 0
            }
        
        state = self._application_states[app_id]
        state["last_event_position"] = event.global_position
        
        # Update state based on event type
        if event.event_type == "ApplicationSubmitted":
            state["submitted_at"] = event.recorded_at.isoformat()
        
        elif event.event_type == "ComplianceRulePassed":
            rule_id = event.payload["rule_id"]
            if rule_id not in state["checks_passed"]:
                state["checks_passed"].append(rule_id)
            state["regulation_versions"].add(event.payload.get("regulation_version", "unknown"))
            state["compliance_status"] = "PARTIAL"
        
        elif event.event_type == "ComplianceRuleFailed":
            rule_id = event.payload["rule_id"]
            if rule_id not in state["checks_failed"]:
                state["checks_failed"].append({
                    "rule_id": rule_id,
                    "reason": event.payload.get("failure_reason"),
                    "timestamp": event.recorded_at.isoformat()
                })
            state["regulation_versions"].add(event.payload.get("regulation_version", "unknown"))
            state["compliance_status"] = "FAILED"
        
        elif event.event_type == "ComplianceCheckInitiated":
            state["required_checks"] = event.payload.get("required_checks", [])
        
        # Update status to PASSED if all required checks passed
        if state.get("required_checks"):
            required = set(state["required_checks"])
            passed = set(state["checks_passed"])
            if required.issubset(passed) and not state["checks_failed"]:
                state["compliance_status"] = "PASSED"
    
    async def _create_snapshot(self, application_id: str, global_position: int) -> None:
        """Create a snapshot of current compliance state"""
        state = self._application_states.get(application_id)
        if not state:
            return
        
        # Convert sets to lists for JSON serialization
        snapshot_data = {
            **state,
            "regulation_versions": list(state["regulation_versions"])
        }
        
        await self._execute("""
            INSERT INTO compliance_audit_snapshots (
                application_id, snapshot_timestamp, snapshot_data, global_position
            ) VALUES ($1, $2, $3, $4)
        """,
            application_id,
            datetime.utcnow(),
            json.dumps(snapshot_data),
            global_position
        )
    
    async def get_compliance_at(
        self, 
        application_id: str, 
        as_of: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get compliance state as it existed at a point in time.
        
        Args:
            application_id: Application identifier
            as_of: Point in time to query (defaults to current)
        
        Returns:
            Compliance state at that time
        """
        if as_of:
            # Find the latest snapshot before the query time
            row = await self._fetchrow("""
                SELECT snapshot_data, global_position
                FROM compliance_audit_snapshots
                WHERE application_id = $1 AND snapshot_timestamp <= $2
                ORDER BY snapshot_timestamp DESC
                LIMIT 1
            """, application_id, as_of)
            
            if row:
                # Need to replay events after snapshot up to as_of
                snapshot_data = row["snapshot_data"]
                position = row["global_position"]
                
                # Replay events after snapshot
                events = []
                async for batch in self.store.load_all(from_global_position=position):
                    for event in batch:
                        if event.recorded_at <= as_of and self._get_application_id(event) == application_id:
                            events.append(event)
                    if events:
                        # Process these events to update state
                        # This would require replaying them into a temporary state
                        pass
                
                return snapshot_data
        
        # Return current state
        state = self._application_states.get(application_id, {})
        return {
            "application_id": application_id,
            "compliance_status": state.get("compliance_status", "UNKNOWN"),
            "checks_passed": state.get("checks_passed", []),
            "checks_failed": state.get("checks_failed", []),
            "submitted_at": state.get("submitted_at"),
            "regulation_versions": list(state.get("regulation_versions", []))
        }
    
    async def get_compliance_summary(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get compliance summary statistics"""
        query = """
            SELECT 
                COUNT(DISTINCT application_id) as total_applications,
                COUNT(CASE WHEN compliance_status = 'PASSED' THEN 1 END) as passed,
                COUNT(CASE WHEN compliance_status = 'FAILED' THEN 1 END) as failed,
                COUNT(CASE WHEN compliance_status = 'PARTIAL' THEN 1 END) as partial,
                COUNT(CASE WHEN compliance_status = 'PENDING' THEN 1 END) as pending
            FROM compliance_audit_snapshots
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND snapshot_timestamp >= $1"
            params.append(start_date)
            if end_date:
                query += " AND snapshot_timestamp <= $2"
                params.append(end_date)
        
        row = await self._fetchrow(query, *params)
        
        return dict(row) if row else {
            "total_applications": 0,
            "passed": 0,
            "failed": 0,
            "partial": 0,
            "pending": 0
        }
    
    def _get_application_id(self, event: StoredEvent) -> Optional[str]:
        """Extract application ID from event payload"""
        if "application_id" in event.payload:
            return event.payload["application_id"]
        
        if event.event_type == "ApplicationSubmitted":
            return event.payload.get("application_id")
        
        return None
    
    async def _clear(self) -> None:
        """Clear all compliance data"""
        await self._execute("TRUNCATE TABLE compliance_audit_snapshots")
        self._application_states.clear()
        self._event_counters.clear()

from collections import defaultdict