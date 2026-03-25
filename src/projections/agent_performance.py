# src/projections/agent_performance.py
"""
Agent Performance Projection - Tracks metrics for AI agents across sessions.
This projection provides insights into agent effectiveness, model performance,
and operational metrics for governance and optimization.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import json
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from src.projections.base import AsyncProjection
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

class AgentStatus(str, Enum):
    """Agent operational status"""
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"

@dataclass
class AgentMetrics:
    """Comprehensive metrics for a single agent model version"""
    agent_id: str
    model_version: str
    total_sessions: int = 0
    total_actions: int = 0
    total_decisions: int = 0
    total_confidence: float = 0.0
    avg_confidence: float = 0.0
    avg_response_time_ms: float = 0.0
    success_rate: float = 100.0
    error_count: int = 0
    token_usage_total: int = 0
    token_usage_avg: float = 0.0
    last_active: Optional[datetime] = None
    first_active: Optional[datetime] = None
    status: AgentStatus = AgentStatus.ACTIVE
    
    # Decision distribution
    approve_count: int = 0
    decline_count: int = 0
    refer_count: int = 0
    
    # Performance over time
    hourly_metrics: Dict[str, Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.hourly_metrics is None:
            self.hourly_metrics = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        result = asdict(self)
        result['status'] = self.status.value
        if self.last_active:
            result['last_active'] = self.last_active.isoformat()
        if self.first_active:
            result['first_active'] = self.first_active.isoformat()
        return result

class AgentPerformanceProjection(AsyncProjection):
    """
    Agent performance projection - tracks metrics per agent and model version.
    This is an async projection for eventual consistency with real-time analytics.
    
    Features:
    - Tracks agent sessions, actions, and decisions
    - Calculates performance metrics (confidence, response time, success rate)
    - Monitors token usage and resource consumption
    - Provides decision distribution analytics
    - Maintains historical performance data
    - Supports time-series analysis
    """
    
    def __init__(self, retention_days: int = 90):
        """
        Initialize agent performance projection.
        
        Args:
            retention_days: Number of days to retain detailed metrics
        """
        super().__init__("agent_performance", batch_size=100)
        self.retention_days = retention_days
        self._metrics_cache: Dict[str, Dict[str, AgentMetrics]] = {}
        self._session_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._hourly_buckets: Dict[str, Dict[str, List[Dict]]] = defaultdict(lambda: defaultdict(list))
        
    async def handle_batch(self, events: List[StoredEvent]) -> None:
        """
        Handle a batch of events and update performance metrics.
        Processes events in batches for efficiency.
        """
        # Group events by agent for batch processing
        agent_events = defaultdict(list)
        
        for event in events:
            agent_id = self._extract_agent_id(event)
            if agent_id:
                agent_events[agent_id].append(event)
        
        # Process each agent's events
        for agent_id, agent_events_list in agent_events.items():
            await self._process_agent_events(agent_id, agent_events_list)
    
    async def _process_agent_events(self, agent_id: str, events: List[StoredEvent]) -> None:
        """
        Process all events for a specific agent.
        
        Args:
            agent_id: Agent identifier
            events: List of events for this agent
        """
        updates = defaultdict(lambda: {
            "total_sessions": 0,
            "total_actions": 0,
            "total_decisions": 0,
            "total_confidence": 0.0,
            "total_response_time": 0.0,
            "error_count": 0,
            "token_usage": 0,
            "approve_count": 0,
            "decline_count": 0,
            "refer_count": 0,
            "last_active": None,
            "first_active": None,
            "response_time_samples": []
        })
        
        session_starts: Dict[str, datetime] = {}
        
        for event in events:
            model_version = self._extract_model_version(event)
            
            # Process based on event type
            if event.event_type == "AgentContextLoaded":
                await self._handle_context_loaded(event, updates, model_version)
                session_starts[event.payload.get("session_id", "")] = event.recorded_at
                
            elif event.event_type == "AgentActionTaken":
                await self._handle_action_taken(event, updates, model_version, session_starts)
                
            elif event.event_type == "DecisionGenerated":
                await self._handle_decision(event, updates, model_version)
                
            elif event.event_type == "AgentError":
                await self._handle_agent_error(event, updates, model_version)
        
        # Apply all updates to database
        await self._apply_updates(agent_id, updates)
        
        # Update hourly metrics
        for event in events:
            await self._update_hourly_metrics(event)
    
    async def _handle_context_loaded(
        self, 
        event: StoredEvent, 
        updates: Dict, 
        model_version: str
    ) -> None:
        """Handle AgentContextLoaded event"""
        payload = event.payload
        session_id = payload.get("session_id", "unknown")
        
        key = (session_id, model_version)
        updates[key]["total_sessions"] += 1
        updates[key]["token_usage"] += payload.get("token_count", 0)
        
        # Track session start time
        if not updates[key]["first_active"]:
            updates[key]["first_active"] = event.recorded_at
        updates[key]["last_active"] = max(
            updates[key]["last_active"] or event.recorded_at,
            event.recorded_at
        )
    
    async def _handle_action_taken(
        self, 
        event: StoredEvent, 
        updates: Dict, 
        model_version: str,
        session_starts: Dict[str, datetime]
    ) -> None:
        """Handle AgentActionTaken event"""
        payload = event.payload
        session_id = payload.get("session_id", "unknown")
        
        key = (session_id, model_version)
        updates[key]["total_actions"] += 1
        
        # Calculate response time if session start is known
        if session_id in session_starts:
            response_time = (event.recorded_at - session_starts[session_id]).total_seconds() * 1000
            updates[key]["total_response_time"] += response_time
            updates[key]["response_time_samples"].append(response_time)
        
        updates[key]["last_active"] = max(
            updates[key]["last_active"] or event.recorded_at,
            event.recorded_at
        )
    
    async def _handle_decision(
        self, 
        event: StoredEvent, 
        updates: Dict, 
        model_version: str
    ) -> None:
        """Handle DecisionGenerated event"""
        payload = event.payload
        recommendation = payload.get("recommendation", "UNKNOWN")
        confidence = payload.get("confidence_score", 0.0)
        
        # Update metrics for each contributing agent session
        for session_id in payload.get("contributing_agent_sessions", []):
            key = (session_id, model_version)
            updates[key]["total_decisions"] += 1
            updates[key]["total_confidence"] += confidence
            
            # Track decision distribution
            if recommendation == "APPROVE":
                updates[key]["approve_count"] += 1
            elif recommendation == "DECLINE":
                updates[key]["decline_count"] += 1
            elif recommendation == "REFER":
                updates[key]["refer_count"] += 1
            
            updates[key]["last_active"] = max(
                updates[key]["last_active"] or event.recorded_at,
                event.recorded_at
            )
    
    async def _handle_agent_error(
        self, 
        event: StoredEvent, 
        updates: Dict, 
        model_version: str
    ) -> None:
        """Handle AgentError event"""
        payload = event.payload
        session_id = payload.get("session_id", "unknown")
        
        key = (session_id, model_version)
        updates[key]["error_count"] += 1
        
        updates[key]["last_active"] = max(
            updates[key]["last_active"] or event.recorded_at,
            event.recorded_at
        )
    
    async def _apply_updates(
        self, 
        agent_id: str, 
        updates: Dict[Tuple[str, str], Dict]
    ) -> None:
        """Apply aggregated updates to database"""
        for (session_id, model_version), metrics in updates.items():
            # Calculate averages
            total_decisions = metrics["total_decisions"]
            avg_confidence = (
                metrics["total_confidence"] / total_decisions 
                if total_decisions > 0 else 0.0
            )
            
            total_actions = metrics["total_actions"]
            avg_response_time = (
                metrics["total_response_time"] / total_actions
                if total_actions > 0 else 0.0
            )
            
            # Calculate success rate
            total_operations = total_actions + total_decisions
            success_rate = (
                ((total_operations - metrics["error_count"]) / total_operations) * 100
                if total_operations > 0 else 100.0
            )
            
            avg_token_usage = (
                metrics["token_usage"] / metrics["total_sessions"]
                if metrics["total_sessions"] > 0 else 0.0
            )
            
            # Determine agent status
            status = AgentStatus.ACTIVE
            if success_rate < 80:
                status = AgentStatus.DEGRADED
            elif metrics["error_count"] > 10:
                status = AgentStatus.ERROR
            elif metrics["total_actions"] == 0 and metrics["total_sessions"] > 0:
                status = AgentStatus.IDLE
            
            # Update database
            await self._execute("""
                INSERT INTO agent_performance (
                    agent_id, model_version, session_id,
                    total_sessions, total_actions, total_decisions,
                    avg_confidence_score, avg_response_time_ms,
                    success_rate, error_count, token_usage_total,
                    token_usage_avg, approve_count, decline_count,
                    refer_count, last_active, first_active, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (agent_id, model_version, session_id) DO UPDATE SET
                    total_sessions = agent_performance.total_sessions + EXCLUDED.total_sessions,
                    total_actions = agent_performance.total_actions + EXCLUDED.total_actions,
                    total_decisions = agent_performance.total_decisions + EXCLUDED.total_decisions,
                    avg_confidence_score = (
                        (agent_performance.avg_confidence_score * agent_performance.total_decisions) +
                        (EXCLUDED.avg_confidence_score * EXCLUDED.total_decisions)
                    ) / NULLIF(agent_performance.total_decisions + EXCLUDED.total_decisions, 0),
                    avg_response_time_ms = (
                        (agent_performance.avg_response_time_ms * agent_performance.total_actions) +
                        (EXCLUDED.avg_response_time_ms * EXCLUDED.total_actions)
                    ) / NULLIF(agent_performance.total_actions + EXCLUDED.total_actions, 0),
                    success_rate = (
                        (agent_performance.success_rate * (agent_performance.total_actions + agent_performance.total_decisions)) +
                        (EXCLUDED.success_rate * (EXCLUDED.total_actions + EXCLUDED.total_decisions))
                    ) / NULLIF(
                        (agent_performance.total_actions + agent_performance.total_decisions) +
                        (EXCLUDED.total_actions + EXCLUDED.total_decisions), 0
                    ),
                    error_count = agent_performance.error_count + EXCLUDED.error_count,
                    token_usage_total = agent_performance.token_usage_total + EXCLUDED.token_usage_total,
                    token_usage_avg = (
                        (agent_performance.token_usage_avg * agent_performance.total_sessions) +
                        (EXCLUDED.token_usage_avg * EXCLUDED.total_sessions)
                    ) / NULLIF(agent_performance.total_sessions + EXCLUDED.total_sessions, 0),
                    approve_count = agent_performance.approve_count + EXCLUDED.approve_count,
                    decline_count = agent_performance.decline_count + EXCLUDED.decline_count,
                    refer_count = agent_performance.refer_count + EXCLUDED.refer_count,
                    last_active = GREATEST(agent_performance.last_active, EXCLUDED.last_active),
                    first_active = LEAST(agent_performance.first_active, EXCLUDED.first_active),
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """,
                agent_id,
                model_version,
                session_id,
                metrics["total_sessions"],
                metrics["total_actions"],
                total_decisions,
                avg_confidence,
                avg_response_time,
                success_rate,
                metrics["error_count"],
                metrics["token_usage"],
                avg_token_usage,
                metrics["approve_count"],
                metrics["decline_count"],
                metrics["refer_count"],
                metrics["last_active"],
                metrics["first_active"],
                status.value
            )
            
            # Update cache
            if agent_id not in self._metrics_cache:
                self._metrics_cache[agent_id] = {}
            
            cache_key = f"{model_version}_{session_id}"
            self._metrics_cache[agent_id][cache_key] = AgentMetrics(
                agent_id=agent_id,
                model_version=model_version,
                total_sessions=metrics["total_sessions"],
                total_actions=metrics["total_actions"],
                total_decisions=total_decisions,
                total_confidence=metrics["total_confidence"],
                avg_confidence=avg_confidence,
                avg_response_time_ms=avg_response_time,
                success_rate=success_rate,
                error_count=metrics["error_count"],
                token_usage_total=metrics["token_usage"],
                token_usage_avg=avg_token_usage,
                last_active=metrics["last_active"],
                first_active=metrics["first_active"],
                status=status,
                approve_count=metrics["approve_count"],
                decline_count=metrics["decline_count"],
                refer_count=metrics["refer_count"]
            )
    
    async def _update_hourly_metrics(self, event: StoredEvent) -> None:
        """Update hourly aggregated metrics for time-series analysis"""
        agent_id = self._extract_agent_id(event)
        if not agent_id:
            return
        
        # Create hourly bucket key
        hour_key = event.recorded_at.strftime("%Y-%m-%d %H:00:00")
        
        # Update metrics in memory
        bucket = self._hourly_buckets[agent_id][hour_key]
        bucket.append({
            "timestamp": event.recorded_at.isoformat(),
            "event_type": event.event_type,
            "agent_id": agent_id,
            "model_version": self._extract_model_version(event)
        })
        
        # Periodically persist hourly aggregates
        if len(bucket) >= 100:
            await self._persist_hourly_metrics(agent_id, hour_key, bucket)
            self._hourly_buckets[agent_id][hour_key] = []
    
    async def _persist_hourly_metrics(self, agent_id: str, hour_key: str, events: List[Dict]) -> None:
        """Persist hourly metrics to database"""
        # Aggregate metrics for the hour
        event_counts = defaultdict(int)
        unique_sessions = set()
        
        for event in events:
            event_counts[event["event_type"]] += 1
            # Would extract session_id if available
        
        await self._execute("""
            INSERT INTO agent_hourly_metrics (
                agent_id, hour_bucket, event_counts, unique_sessions, updated_at
            ) VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (agent_id, hour_bucket) DO UPDATE SET
                event_counts = agent_hourly_metrics.event_counts || EXCLUDED.event_counts,
                unique_sessions = agent_hourly_metrics.unique_sessions + EXCLUDED.unique_sessions,
                updated_at = NOW()
        """,
            agent_id,
            hour_key,
            json.dumps(dict(event_counts)),
            len(unique_sessions)
        )
    
    # Query Methods
    
    async def get_agent_metrics(
        self, 
        agent_id: str, 
        model_version: Optional[str] = None,
        session_id: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get metrics for a specific agent with optional filters.
        
        Args:
            agent_id: Agent identifier
            model_version: Optional model version filter
            session_id: Optional session ID filter
            time_range: Optional time range tuple (start, end)
        
        Returns:
            List of metrics dictionaries
        """
        query = """
            SELECT * FROM agent_performance
            WHERE agent_id = $1
        """
        params = [agent_id]
        param_index = 2
        
        if model_version:
            query += f" AND model_version = ${param_index}"
            params.append(model_version)
            param_index += 1
        
        if session_id:
            query += f" AND session_id = ${param_index}"
            params.append(session_id)
            param_index += 1
        
        if time_range:
            start, end = time_range
            query += f" AND last_active BETWEEN ${param_index} AND ${param_index + 1}"
            params.extend([start, end])
        
        query += " ORDER BY last_active DESC"
        
        rows = await self._fetch(query, *params)
        return [dict(row) for row in rows]
    
    async def get_agent_summary(self, agent_id: str) -> Dict[str, Any]:
        """
        Get aggregated summary for an agent across all versions.
        
        Args:
            agent_id: Agent identifier
        
        Returns:
            Dictionary with aggregated metrics
        """
        rows = await self._fetch("""
            SELECT 
                COUNT(DISTINCT model_version) as version_count,
                SUM(total_sessions) as total_sessions,
                SUM(total_actions) as total_actions,
                SUM(total_decisions) as total_decisions,
                AVG(avg_confidence_score) as avg_confidence,
                AVG(avg_response_time_ms) as avg_response_time,
                AVG(success_rate) as avg_success_rate,
                SUM(error_count) as total_errors,
                SUM(token_usage_total) as total_tokens,
                SUM(approve_count) as total_approvals,
                SUM(decline_count) as total_declines,
                SUM(refer_count) as total_refers,
                MAX(last_active) as last_active,
                MIN(first_active) as first_active
            FROM agent_performance
            WHERE agent_id = $1
        """, agent_id)
        
        if not rows:
            return {"agent_id": agent_id, "total_sessions": 0}
        
        result = dict(rows[0])
        result["agent_id"] = agent_id
        
        # Calculate decision distribution percentages
        total_decisions = result.get("total_decisions", 0)
        if total_decisions > 0:
            result["approval_rate"] = (result.get("total_approvals", 0) / total_decisions) * 100
            result["decline_rate"] = (result.get("total_declines", 0) / total_decisions) * 100
            result["refer_rate"] = (result.get("total_refers", 0) / total_decisions) * 100
        
        return result
    
    async def get_top_agents(
        self, 
        limit: int = 10,
        metric: str = "total_decisions",
        order_desc: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get top performing agents by specified metric.
        
        Args:
            limit: Maximum number of agents to return
            metric: Metric to sort by (total_decisions, avg_confidence, success_rate, etc.)
            order_desc: Sort in descending order if True
        
        Returns:
            List of top agents with metrics
        """
        order_direction = "DESC" if order_desc else "ASC"
        
        rows = await self._fetch(f"""
            SELECT 
                agent_id,
                SUM(total_sessions) as total_sessions,
                SUM(total_decisions) as total_decisions,
                AVG(avg_confidence_score) as avg_confidence,
                AVG(success_rate) as avg_success_rate,
                MAX(last_active) as last_active
            FROM agent_performance
            GROUP BY agent_id
            ORDER BY {metric} {order_direction}
            LIMIT $1
        """, limit)
        
        return [dict(row) for row in rows]
    
    async def get_agent_performance_over_time(
        self, 
        agent_id: str,
        days: int = 30,
        resolution: str = "hour"
    ) -> List[Dict[str, Any]]:
        """
        Get agent performance metrics over time for trend analysis.
        
        Args:
            agent_id: Agent identifier
            days: Number of days to look back
            resolution: Time resolution (hour, day, week)
        
        Returns:
            List of time-series metrics
        """
        interval = "1 hour" if resolution == "hour" else "1 day" if resolution == "day" else "1 week"
        
        rows = await self._fetch("""
            SELECT 
                DATE_TRUNC($2, recorded_at) as time_bucket,
                COUNT(*) as event_count,
                COUNT(DISTINCT CASE WHEN event_type = 'AgentContextLoaded' THEN session_id END) as active_sessions,
                COUNT(CASE WHEN event_type = 'AgentActionTaken' THEN 1 END) as action_count,
                COUNT(CASE WHEN event_type = 'DecisionGenerated' THEN 1 END) as decision_count,
                COUNT(CASE WHEN event_type = 'AgentError' THEN 1 END) as error_count
            FROM events
            WHERE stream_id LIKE $1 AND recorded_at >= NOW() - $3::interval
            GROUP BY time_bucket
            ORDER BY time_bucket DESC
        """, f"agent-{agent_id}%", resolution, f"{days} days")
        
        results = []
        for row in rows:
            result = dict(row)
            result["success_rate"] = (
                ((result["event_count"] - result["error_count"]) / result["event_count"]) * 100
                if result["event_count"] > 0 else 100.0
            )
            results.append(result)
        
        return results
    
    async def get_agent_model_comparison(
        self, 
        agent_id: str
    ) -> List[Dict[str, Any]]:
        """
        Compare performance across different model versions for an agent.
        
        Args:
            agent_id: Agent identifier
        
        Returns:
            List of model version comparisons
        """
        rows = await self._fetch("""
            SELECT 
                model_version,
                COUNT(DISTINCT session_id) as session_count,
                SUM(total_decisions) as total_decisions,
                AVG(avg_confidence_score) as avg_confidence,
                AVG(success_rate) as success_rate,
                AVG(avg_response_time_ms) as avg_response_time,
                SUM(error_count) as total_errors,
                AVG(token_usage_avg) as avg_token_usage,
                MAX(last_active) as last_used
            FROM agent_performance
            WHERE agent_id = $1
            GROUP BY model_version
            ORDER BY last_used DESC
        """, agent_id)
        
        return [dict(row) for row in rows]
    
    async def get_agent_health_check(self) -> List[Dict[str, Any]]:
        """
        Get health status for all agents.
        Identifies agents that may need attention.
        
        Returns:
            List of agents with health issues
        """
        rows = await self._fetch("""
            SELECT 
                agent_id,
                model_version,
                status,
                success_rate,
                error_count,
                total_decisions,
                last_active,
                CASE 
                    WHEN NOW() - last_active > INTERVAL '1 hour' THEN 'INACTIVE'
                    WHEN success_rate < 70 THEN 'CRITICAL'
                    WHEN success_rate < 85 THEN 'WARNING'
                    ELSE 'HEALTHY'
                END as health_status
            FROM agent_performance
            WHERE status != 'ACTIVE' 
               OR success_rate < 85
               OR NOW() - last_active > INTERVAL '1 hour'
            ORDER BY 
                CASE 
                    WHEN success_rate < 70 THEN 1
                    WHEN success_rate < 85 THEN 2
                    ELSE 3
                END,
                last_active DESC
        """)
        
        return [dict(row) for row in rows]
    
    async def get_agent_anomalies(
        self, 
        agent_id: str,
        lookback_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies in agent behavior.
        
        Args:
            agent_id: Agent identifier
            lookback_hours: Hours to look back for analysis
        
        Returns:
            List of detected anomalies
        """
        # Get recent metrics
        recent = await self.get_agent_performance_over_time(
            agent_id, 
            days=lookback_hours / 24,
            resolution="hour"
        )
        
        # Calculate baseline metrics (previous 7 days)
        baseline = await self._fetch("""
            SELECT 
                AVG(decision_count) as avg_decisions,
                STDDEV(decision_count) as stddev_decisions,
                AVG(error_count) as avg_errors,
                STDDEV(error_count) as stddev_errors
            FROM (
                SELECT 
                    DATE_TRUNC('hour', recorded_at) as hour,
                    COUNT(*) as event_count,
                    COUNT(CASE WHEN event_type = 'DecisionGenerated' THEN 1 END) as decision_count,
                    COUNT(CASE WHEN event_type = 'AgentError' THEN 1 END) as error_count
                FROM events
                WHERE stream_id LIKE $1 
                  AND recorded_at >= NOW() - INTERVAL '7 days'
                  AND recorded_at < NOW() - INTERVAL '1 day'
                GROUP BY hour
            ) hourly_metrics
        """, f"agent-{agent_id}%")
        
        if not baseline or not baseline[0]:
            return []
        
        baseline_row = baseline[0]
        avg_decisions = baseline_row.get("avg_decisions", 0) or 0
        stddev_decisions = baseline_row.get("stddev_decisions", 0) or 1
        avg_errors = baseline_row.get("avg_errors", 0) or 0
        stddev_errors = baseline_row.get("stddev_errors", 0) or 1
        
        # Detect anomalies
        anomalies = []
        for hour_data in recent:
            decisions = hour_data.get("decision_count", 0)
            errors = hour_data.get("error_count", 0)
            
            # Detect decision anomalies (3 sigma rule)
            if decisions > avg_decisions + (3 * stddev_decisions):
                anomalies.append({
                    "timestamp": hour_data["time_bucket"],
                    "type": "DECISION_SPIKE",
                    "severity": "HIGH",
                    "value": decisions,
                    "baseline": avg_decisions,
                    "deviation": decisions - avg_decisions,
                    "message": f"Unusual decision volume: {decisions} vs baseline {avg_decisions:.1f}"
                })
            elif decisions < avg_decisions - (2 * stddev_decisions) and decisions > 0:
                anomalies.append({
                    "timestamp": hour_data["time_bucket"],
                    "type": "DECISION_DROP",
                    "severity": "MEDIUM",
                    "value": decisions,
                    "baseline": avg_decisions,
                    "deviation": decisions - avg_decisions,
                    "message": f"Decision volume drop: {decisions} vs baseline {avg_decisions:.1f}"
                })
            
            # Detect error anomalies
            if errors > avg_errors + (2 * stddev_errors):
                anomalies.append({
                    "timestamp": hour_data["time_bucket"],
                    "type": "ERROR_SPIKE",
                    "severity": "HIGH",
                    "value": errors,
                    "baseline": avg_errors,
                    "deviation": errors - avg_errors,
                    "message": f"Error rate spike: {errors} errors vs baseline {avg_errors:.1f}"
                })
        
        return anomalies
    
    async def export_metrics(
        self, 
        agent_id: str,
        format: str = "json",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Export agent metrics for reporting or analysis.
        
        Args:
            agent_id: Agent identifier
            format: Export format (json, csv)
            start_date: Start date for export
            end_date: End date for export
        
        Returns:
            Exported metrics data
        """
        if not end_date:
            end_date = datetime.utcnow()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        # Get summary metrics
        summary = await self.get_agent_summary(agent_id)
        
        # Get detailed metrics
        metrics = await self.get_agent_metrics(
            agent_id,
            time_range=(start_date, end_date)
        )
        
        # Get time-series data
        timeseries = await self.get_agent_performance_over_time(
            agent_id,
            days=(end_date - start_date).days
        )
        
        # Get model comparison
        models = await self.get_agent_model_comparison(agent_id)
        
        # Get anomalies
        anomalies = await self.get_agent_anomalies(agent_id)
        
        export_data = {
            "exported_at": datetime.utcnow().isoformat(),
            "agent_id": agent_id,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "summary": summary,
            "model_comparison": models,
            "detailed_metrics": metrics,
            "time_series": timeseries,
            "anomalies": anomalies,
            "total_events": len(metrics)
        }
        
        return export_data
    
    def _extract_agent_id(self, event: StoredEvent) -> Optional[str]:
        """Extract agent ID from event payload"""
        if "agent_id" in event.payload:
            return event.payload["agent_id"]
        
        # Try to extract from stream_id
        if event.stream_id.startswith("agent-"):
            parts = event.stream_id.split("-")
            if len(parts) >= 2:
                return parts[1]
        
        # For decision events, extract from contributing sessions
        if event.event_type == "DecisionGenerated":
            sessions = event.payload.get("contributing_agent_sessions", [])
            if sessions:
                # Return first agent from sessions
                session = sessions[0]
                parts = session.split("-")
                if len(parts) >= 2:
                    return parts[1]
        
        return None
    
    def _extract_model_version(self, event: StoredEvent) -> str:
        """Extract model version from event payload"""
        if "model_version" in event.payload:
            return event.payload["model_version"]
        
        if event.event_type == "AgentContextLoaded":
            return event.payload.get("model_version", "unknown")
        
        if event.event_type == "CreditAnalysisCompleted":
            return event.payload.get("model_version", "unknown")
        
        return "unknown"
    
    async def _clear(self) -> None:
        """Clear all performance data"""
        await self._execute("TRUNCATE TABLE agent_performance")
        await self._execute("TRUNCATE TABLE agent_hourly_metrics")
        self._metrics_cache.clear()
        self._session_metrics.clear()
        self._hourly_buckets.clear()
        logger.info("Agent performance projection cleared")