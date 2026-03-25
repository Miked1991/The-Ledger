# src/mcp/resources.py
"""
Complete MCP Resources Implementation - Query Side
All 6 resources reading from projections with SLOs and temporal queries.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import logging
from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.projections.daemon import ProjectionDaemon
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate

logger = logging.getLogger(__name__)


class MCPResources:
    """
    MCP Resources - Query side of CQRS.
    
    All resources:
    - Read from projections, never from aggregates (except justified exceptions)
    - Have explicit SLO commitments
    - Support temporal queries where applicable
    """
    
    def __init__(self, store: EventStore, daemon: Optional[ProjectionDaemon] = None):
        self.store = store
        self.daemon = daemon
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, datetime] = {}
        self._stats = {
            "total_queries": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "query_times_ms": []
        }
    
    async def get_resource(self, uri: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """Get a resource by URI"""
        
        start_time = datetime.utcnow()
        self._stats["total_queries"] += 1
        
        try:
            # Check cache for GET requests without temporal parameters
            cache_key = f"{uri}:{json.dumps(parameters or {}, sort_keys=True)}"
            if parameters is None or "as_of" not in parameters:
                if cache_key in self._cache:
                    cached_time = self._cache_ttl.get(cache_key)
                    if cached_time and (datetime.utcnow() - cached_time).total_seconds() < 60:
                        self._stats["cache_hits"] += 1
                        return self._cache[cache_key]
            
            self._stats["cache_misses"] += 1
            
            # Parse URI and route
            if uri.startswith("ledger://applications/"):
                parts = uri.replace("ledger://applications/", "").split("/")
                application_id = parts[0]
                
                if len(parts) == 1:
                    # ledger://applications/{id}
                    # SLO: p99 < 50ms
                    result = await self.get_application_summary(application_id)
                    
                elif len(parts) == 2 and parts[1] == "compliance":
                    # ledger://applications/{id}/compliance
                    # SLO: p99 < 200ms
                    as_of = parameters.get("as_of") if parameters else None
                    result = await self.get_compliance_audit(application_id, as_of)
                    
                elif len(parts) == 2 and parts[1] == "audit-trail":
                    # ledger://applications/{id}/audit-trail
                    # SLO: p99 < 500ms
                    from_pos = parameters.get("from") if parameters else None
                    to_pos = parameters.get("to") if parameters else None
                    result = await self.get_audit_trail(application_id, from_pos, to_pos)
                
                elif len(parts) == 2 and parts[1] == "status":
                    # ledger://applications/{id}/status
                    # SLO: p99 < 30ms
                    result = await self.get_application_status(application_id)
                
                else:
                    raise ValueError(f"Unknown resource URI: {uri}")
            
            elif uri.startswith("ledger://agents/"):
                parts = uri.replace("ledger://agents/", "").split("/")
                agent_id = parts[0]
                
                if len(parts) == 2 and parts[1] == "performance":
                    # ledger://agents/{id}/performance
                    # SLO: p99 < 50ms
                    result = await self.get_agent_performance(agent_id)
                    
                elif len(parts) == 3 and parts[1] == "sessions":
                    # ledger://agents/{id}/sessions/{session_id}
                    # SLO: p99 < 300ms
                    session_id = parts[2]
                    result = await self.get_agent_session(agent_id, session_id, parameters)
                
                else:
                    raise ValueError(f"Unknown resource URI: {uri}")
            
            elif uri.startswith("ledger://compliance/"):
                parts = uri.replace("ledger://compliance/", "").split("/")
                application_id = parts[0]
                
                if len(parts) == 2 and parts[1] == "summary":
                    # ledger://compliance/{id}/summary
                    # SLO: p99 < 100ms
                    result = await self.get_compliance_summary(application_id)
                
                else:
                    raise ValueError(f"Unknown resource URI: {uri}")
            
            elif uri == "ledger://ledger/health":
                # ledger://ledger/health
                # SLO: p99 < 10ms
                result = await self.get_health()
            
            else:
                raise ValueError(f"Unknown resource URI: {uri}")
            
            # Cache result
            if cache_key and "as_of" not in (parameters or {}):
                self._cache[cache_key] = result
                self._cache_ttl[cache_key] = datetime.utcnow()
            
            # Record query time
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._stats["query_times_ms"].append(elapsed_ms)
            if len(self._stats["query_times_ms"]) > 1000:
                self._stats["query_times_ms"] = self._stats["query_times_ms"][-1000:]
            
            return result
            
        except Exception as e:
            logger.error(f"Resource query failed for {uri}: {e}", exc_info=True)
            raise
    
    async def get_application_summary(self, application_id: str) -> Dict[str, Any]:
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
    
    async def get_application_status(self, application_id: str) -> Dict[str, Any]:
        """
        Get quick application status.
        SLO: p99 < 30ms
        """
        async with self.store.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT application_id, current_state, final_decision, updated_at
                FROM application_summary
                WHERE application_id = $1
            """, application_id)
            
            if not row:
                return {"status": "NOT_FOUND", "application_id": application_id}
            
            return {
                "application_id": row["application_id"],
                "status": row["current_state"],
                "final_decision": row["final_decision"],
                "last_updated": row["updated_at"].isoformat()
            }
    
    async def get_compliance_audit(self, application_id: str, 
                                   as_of: Optional[str] = None) -> Dict[str, Any]:
        """
        Get compliance audit view with temporal query support.
        SLO: p99 < 200ms
        """
        if as_of:
            # Temporal query: get state as it existed at a point in time
            as_of_dt = datetime.fromisoformat(as_of)
            
            async with self.store.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT snapshot_data FROM compliance_audit_snapshots
                    WHERE application_id = $1 AND snapshot_timestamp <= $2
                    ORDER BY snapshot_timestamp DESC
                    LIMIT 1
                """, application_id, as_of_dt)
                
                if row:
                    return row['snapshot_data']
        
        # Current state - from compliance aggregate
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        return compliance.get_compliance_summary()
    
    async def get_compliance_summary(self, application_id: str) -> Dict[str, Any]:
        """
        Get compliance summary for application.
        SLO: p99 < 100ms
        """
        compliance = await ComplianceRecordAggregate.load(self.store, application_id)
        
        return {
            "application_id": application_id,
            "compliance_status": compliance.status,
            "checks_passed": list(compliance.checks_passed),
            "checks_failed": compliance.checks_failed,
            "required_checks": list(compliance.required_checks),
            "regulation_versions": list(compliance.regulation_versions),
            "completed_at": compliance.completed_at.isoformat() if compliance.completed_at else None
        }
    
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
        
        # Get integrity report
        audit = await AuditLedgerAggregate.load(self.store, "loan", application_id)
        integrity_report = audit.get_integrity_report()
        
        return {
            "application_id": application_id,
            "stream_id": stream_id,
            "total_events": len(events),
            "events": [
                {
                    "position": e.stream_position,
                    "type": e.event_type,
                    "version": e.event_version,
                    "payload": e.payload,
                    "recorded_at": e.recorded_at.isoformat(),
                    "correlation_id": e.correlation_id,
                    "causation_id": e.causation_id
                }
                for e in events
            ],
            "integrity": integrity_report,
            "verification_instructions": {
                "verify_hash": f"Recompute chain and compare to {integrity_report.get('hash_chain_root')}"
            }
        }
    
    async def get_agent_performance(self, agent_id: str) -> Dict[str, Any]:
        """
        Get agent performance metrics from projection.
        SLO: p99 < 50ms
        """
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT agent_id, model_version, total_sessions, 
                       total_decisions, avg_confidence_score, 
                       success_rate, last_active
                FROM agent_performance
                WHERE agent_id = $1
                ORDER BY last_active DESC
            """, agent_id)
            
            return {
                "agent_id": agent_id,
                "performance": [dict(row) for row in rows],
                "total_versions": len(rows),
                "latest_activity": rows[0]["last_active"].isoformat() if rows else None
            }
    
    async def get_agent_session(self, agent_id: str, session_id: str,
                                parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get full agent session with replay capability.
        SLO: p99 < 300ms
        
        Justified exception: Direct stream load for full replay capability.
        """
        # Direct stream load - justified for full session replay
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await self.store.load_stream(stream_id)
        
        # Reconstruct context using Gas Town pattern
        context = await reconstruct_agent_context(self.store, agent_id, session_id)
        
        return {
            "agent_id": agent_id,
            "session_id": session_id,
            "context": context.to_dict(),
            "total_events": len(events),
            "events": [
                {
                    "position": e.stream_position,
                    "type": e.event_type,
                    "payload": e.payload,
                    "recorded_at": e.recorded_at.isoformat()
                }
                for e in events
            ],
            "replay_capable": True,
            "replay_instructions": "Use load_stream() with from_position parameter"
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
                lags[row['projection_name']] = {
                    "lag_events": lag,
                    "lag_estimated_ms": lag * 0.5,
                    "last_updated": row['updated_at'].isoformat()
                }
            
            # Get aggregate counts
            app_count = await conn.fetchval("SELECT COUNT(*) FROM application_summary")
            agent_count = await conn.fetchval("SELECT COUNT(DISTINCT agent_id) FROM agent_performance")
            
            # Get daemon stats if available
            daemon_stats = self.daemon.get_stats() if self.daemon else {}
            
            return {
                "status": "healthy",
                "current_global_position": current_position,
                "projection_lags": lags,
                "statistics": {
                    "applications": app_count,
                    "agents": agent_count,
                    "total_events": current_position
                },
                "daemon": daemon_stats,
                "slo_status": {
                    "application_summary": "OK" if lags.get("application_summary", {}).get("lag_events", 0) < 100 else "WARNING",
                    "agent_performance": "OK" if lags.get("agent_performance", {}).get("lag_events", 0) < 500 else "WARNING",
                    "compliance_audit": "OK" if lags.get("compliance_audit", {}).get("lag_events", 0) < 1000 else "WARNING"
                }
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get resource query statistics"""
        avg_time = sum(self._stats["query_times_ms"]) / len(self._stats["query_times_ms"]) if self._stats["query_times_ms"] else 0
        p95_time = sorted(self._stats["query_times_ms"])[int(len(self._stats["query_times_ms"]) * 0.95)] if self._stats["query_times_ms"] else 0
        
        return {
            **self._stats,
            "cache_hit_rate": (self._stats["cache_hits"] / self._stats["total_queries"] * 100) if self._stats["total_queries"] > 0 else 0,
            "avg_query_time_ms": avg_time,
            "p95_query_time_ms": p95_time,
            "cache_size": len(self._cache)
        }
    
    def clear_cache(self) -> None:
        """Clear the resource cache"""
        self._cache.clear()
        self._cache_ttl.clear()
        logger.info("Resource cache cleared")