# src/mcp/resources.py
from typing import Dict, Any, Optional
from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.projections.daemon import ProjectionDaemon

class MCPResources:
    """
    MCP Resources - Query side of CQRS.
    All reads must come from projections, never from replaying aggregates.
    """
    
    def __init__(self, store: EventStore):
        self.store = store
        self.daemon: Optional[ProjectionDaemon] = None
    
    async def get_resource(self, uri: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """Get a resource by URI"""
        
        # Parse URI: ledger://applications/{id}
        if uri.startswith("ledger://applications/"):
            parts = uri.replace("ledger://applications/", "").split("/")
            application_id = parts[0]
            
            if len(parts) == 1:
                # ledger://applications/{id}
                return await self.get_application_summary(application_id, parameters)
            elif len(parts) == 2 and parts[1] == "compliance":
                # ledger://applications/{id}/compliance
                as_of = parameters.get("as_of") if parameters else None
                return await self.get_compliance_audit(application_id, as_of)
            elif len(parts) == 2 and parts[1] == "audit-trail":
                # ledger://applications/{id}/audit-trail
                from_pos = parameters.get("from") if parameters else None
                to_pos = parameters.get("to") if parameters else None
                return await self.get_audit_trail(application_id, from_pos, to_pos)
        
        elif uri.startswith("ledger://agents/"):
            parts = uri.replace("ledger://agents/", "").split("/")
            agent_id = parts[0]
            
            if len(parts) == 2 and parts[1] == "performance":
                # ledger://agents/{id}/performance
                return await self.get_agent_performance(agent_id)
            elif len(parts) == 3 and parts[1] == "sessions":
                # ledger://agents/{id}/sessions/{session_id}
                session_id = parts[2]
                return await self.get_agent_session(agent_id, session_id, parameters)
        
        elif uri == "ledger://ledger/health":
            return await self.get_health()
        
        raise ValueError(f"Unknown resource URI: {uri}")
    
    async def get_application_summary(self, application_id: str, 
                                      parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get current application summary from projection.
        SLO: p99 < 50ms
        """
        async with self.store.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM application_summary
                WHERE application_id = $1
            """, application_id)
            
            if not row:
                raise ValueError(f"Application {application_id} not found")
            
            return dict(row)
    
    async def get_compliance_audit(self, application_id: str, 
                                   as_of: Optional[str] = None) -> Dict[str, Any]:
        """
        Get compliance audit view with temporal query support.
        SLO: p99 < 200ms
        """
        if as_of:
            # Temporal query: get state as it existed at a point in time
            async with self.store.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT snapshot_data FROM compliance_audit_snapshots
                    WHERE application_id = $1 AND snapshot_timestamp <= $2
                    ORDER BY snapshot_timestamp DESC
                    LIMIT 1
                """, application_id, as_of)
                
                if row:
                    return row['snapshot_data']
        
        # Current state - rebuild from events
        stream_id = f"loan-{application_id}"
        events = await self.store.load_stream(stream_id)
        
        # Process events to build compliance view
        compliance_state = {
            "application_id": application_id,
            "checks_passed": [],
            "checks_failed": [],
            "compliance_status": "PENDING",
            "audit_events": []
        }
        
        for event in events:
            if event.event_type == "ComplianceRulePassed":
                compliance_state["checks_passed"].append(event.payload["rule_id"])
            elif event.event_type == "ComplianceRuleFailed":
                compliance_state["checks_failed"].append(event.payload["rule_id"])
            elif event.event_type == "ApplicationSubmitted":
                compliance_state["submitted_at"] = event.recorded_at.isoformat()
        
        compliance_state["compliance_status"] = "PASSED" if len(compliance_state["checks_failed"]) == 0 else "FAILED"
        
        return compliance_state
    
    async def get_audit_trail(self, application_id: str, 
                              from_pos: Optional[int] = None,
                              to_pos: Optional[int] = None) -> Dict[str, Any]:
        """
        Get complete audit trail for application.
        SLO: p99 < 500ms
        """
        stream_id = f"loan-{application_id}"
        from_pos = from_pos or 0
        events = await self.store.load_stream(stream_id, from_position=from_pos, to_position=to_pos)
        
        return {
            "application_id": application_id,
            "stream_id": stream_id,
            "events": [
                {
                    "position": e.stream_position,
                    "type": e.event_type,
                    "version": e.event_version,
                    "payload": e.payload,
                    "recorded_at": e.recorded_at.isoformat()
                }
                for e in events
            ],
            "total_events": len(events)
        }
    
    async def get_agent_performance(self, agent_id: str) -> Dict[str, Any]:
        """
        Get agent performance metrics from projection.
        SLO: p99 < 50ms
        """
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT agent_id, model_version, total_sessions, 
                       total_decisions, avg_confidence_score, last_active
                FROM agent_performance
                WHERE agent_id = $1
            """, agent_id)
            
            return {
                "agent_id": agent_id,
                "performance": [dict(row) for row in rows]
            }
    
    async def get_agent_session(self, agent_id: str, session_id: str,
                                parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get full agent session with replay capability.
        SLO: p99 < 300ms
        """
        # Direct stream load - justified exception for full replay
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await self.store.load_stream(stream_id)
        
        # Reconstruct context using Gas Town pattern
        context = await reconstruct_agent_context(self.store, agent_id, session_id)
        
        return {
            "agent_id": agent_id,
            "session_id": session_id,
            "context": context.to_dict(),
            "events": [
                {
                    "position": e.stream_position,
                    "type": e.event_type,
                    "payload": e.payload,
                    "recorded_at": e.recorded_at.isoformat()
                }
                for e in events
            ]
        }
    
    async def get_health(self) -> Dict[str, Any]:
        """
        Get system health including projection lag metrics.
        SLO: p99 < 10ms
        """
        async with self.store.pool.acquire() as conn:
            # Get current global position
            row = await conn.fetchrow("SELECT MAX(global_position) FROM events")
            current_position = row[0] or 0
            
            # Get checkpoints
            rows = await conn.fetch("SELECT * FROM projection_checkpoints")
            
            lags = {}
            for row in rows:
                lag = current_position - row['last_position']
                lags[row['projection_name']] = lag
            
            return {
                "status": "healthy",
                "current_global_position": current_position,
                "projection_lags": lags,
                "daemon_running": self.daemon is not None and self.daemon._running
            }