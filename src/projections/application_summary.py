# src/projections/application_summary.py
"""
Application Summary Projection - Maintains current state of each application.
This is an inline projection for strong consistency in real-time queries.
"""

from typing import Dict, Any, Optional, List
import json
import asyncpg
from src.projections.base import InlineProjection
from src.models.events import StoredEvent
from src.models.events import ApplicationState


class ApplicationSummaryProjection(InlineProjection):
    """
    Application summary projection - maintains current state of each application.
    This is an inline projection for strong consistency.
    SLO: p99 < 50ms
    """
    
    def __init__(self):
        super().__init__("application_summary")
        self._cache: Dict[str, Dict[str, Any]] = {}  # In-memory cache for hot data
    
    async def _handle_event_in_transaction(self, event: StoredEvent) -> None:
        """Handle event within the append transaction"""
        if event.event_type == "ApplicationSubmitted":
            await self._handle_submitted(event)
        elif event.event_type == "CreditAnalysisCompleted":
            await self._handle_credit_analysis(event)
        elif event.event_type == "FraudScreeningCompleted":
            await self._handle_fraud_screening(event)
        elif event.event_type == "ComplianceRulePassed":
            await self._handle_compliance_passed(event)
        elif event.event_type == "ComplianceRuleFailed":
            await self._handle_compliance_failed(event)
        elif event.event_type == "DecisionGenerated":
            await self._handle_decision(event)
        elif event.event_type == "HumanReviewCompleted":
            await self._handle_human_review(event)
    
    async def _handle_submitted(self, event: StoredEvent) -> None:
        """Handle application submission"""
        payload = event.payload
        await self._execute("""
            INSERT INTO application_summary (
                application_id, applicant_id, requested_amount,
                business_name, tax_id, current_state, 
                submitted_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (application_id) DO UPDATE SET
                applicant_id = EXCLUDED.applicant_id,
                requested_amount = EXCLUDED.requested_amount,
                business_name = EXCLUDED.business_name,
                tax_id = EXCLUDED.tax_id,
                current_state = EXCLUDED.current_state,
                updated_at = NOW()
        """, 
            payload["application_id"],
            payload["applicant_id"],
            payload["requested_amount_usd"],
            payload["business_name"],
            payload["tax_id"],
            ApplicationState.SUBMITTED.value,
            event.recorded_at
        )
        
        # Update cache
        self._cache[payload["application_id"]] = {
            "application_id": payload["application_id"],
            "current_state": ApplicationState.SUBMITTED.value,
            "requested_amount": payload["requested_amount_usd"],
            "applicant_id": payload["applicant_id"]
        }
    
    async def _handle_credit_analysis(self, event: StoredEvent) -> None:
        """Handle credit analysis completion"""
        payload = event.payload
        await self._execute("""
            UPDATE application_summary
            SET credit_analysis_tier = $1,
                credit_score = $2,
                max_credit_limit = $3,
                credit_analysis_model = $4,
                credit_analysis_confidence = $5,
                current_state = $6,
                updated_at = NOW()
            WHERE application_id = $7
        """,
            payload["risk_tier"],
            payload["credit_score"],
            payload["max_credit_limit"],
            payload.get("model_version"),
            payload.get("confidence_score"),
            ApplicationState.ANALYSIS_COMPLETE.value,
            payload["application_id"]
        )
        
        # Update cache
        if payload["application_id"] in self._cache:
            self._cache[payload["application_id"]]["credit_analysis_tier"] = payload["risk_tier"]
            self._cache[payload["application_id"]]["current_state"] = ApplicationState.ANALYSIS_COMPLETE.value
    
    async def _handle_fraud_screening(self, event: StoredEvent) -> None:
        """Handle fraud screening completion"""
        payload = event.payload
        await self._execute("""
            UPDATE application_summary
            SET fraud_score = $1,
                fraud_flags = $2,
                fraud_risk_indicators = $3,
                updated_at = NOW()
            WHERE application_id = $4
        """,
            payload["fraud_score"],
            payload["flags"],
            json.dumps(payload.get("risk_indicators", {})),
            payload["application_id"]
        )
    
    async def _handle_compliance_passed(self, event: StoredEvent) -> None:
        """Handle compliance rule passed"""
        payload = event.payload
        await self._execute("""
            UPDATE application_summary
            SET compliance_checks_passed = 
                COALESCE(compliance_checks_passed, '[]'::jsonb) || $1::jsonb,
                updated_at = NOW()
            WHERE application_id = $2
        """,
            f'["{payload["rule_id"]}"]',
            payload["application_id"]
        )
    
    async def _handle_compliance_failed(self, event: StoredEvent) -> None:
        """Handle compliance rule failed"""
        payload = event.payload
        await self._execute("""
            UPDATE application_summary
            SET compliance_checks_failed = 
                COALESCE(compliance_checks_failed, '[]'::jsonb) || $1::jsonb,
                updated_at = NOW()
            WHERE application_id = $2
        """,
            f'{{"rule_id": "{payload["rule_id"]}", "reason": "{payload.get("failure_reason", "")}"}}',
            payload["application_id"]
        )
    
    async def _handle_decision(self, event: StoredEvent) -> None:
        """Handle decision generation"""
        payload = event.payload
        new_state = (
            ApplicationState.APPROVED_PENDING_HUMAN.value 
            if payload["recommendation"] == "APPROVE"
            else ApplicationState.DECLINED_PENDING_HUMAN.value
            if payload["recommendation"] == "DECLINE"
            else ApplicationState.PENDING_DECISION.value
        )
        
        await self._execute("""
            UPDATE application_summary
            SET ai_recommendation = $1,
                ai_confidence = $2,
                decision_reasoning = $3,
                contributing_sessions = $4,
                current_state = $5,
                updated_at = NOW()
            WHERE application_id = $6
        """,
            payload["recommendation"],
            payload["confidence_score"],
            payload["decision_reasoning"],
            payload.get("contributing_agent_sessions", []),
            new_state,
            payload["application_id"]
        )
    
    async def _handle_human_review(self, event: StoredEvent) -> None:
        """Handle human review completion"""
        payload = event.payload
        final_state = (
            ApplicationState.FINAL_APPROVED.value 
            if payload["final_decision"] == "APPROVED"
            else ApplicationState.FINAL_DECLINED.value
        )
        
        await self._execute("""
            UPDATE application_summary
            SET final_decision = $1,
                current_state = $2,
                human_reviewer_id = $3,
                human_review_comments = $4,
                override = $5,
                override_reason = $6,
                reviewed_at = $7,
                updated_at = NOW()
            WHERE application_id = $8
        """,
            payload["final_decision"],
            final_state,
            payload["reviewer_id"],
            payload.get("comments"),
            payload.get("override", False),
            payload.get("override_reason"),
            event.recorded_at,
            payload["application_id"]
        )
    
    async def get_application(self, application_id: str) -> Optional[Dict[str, Any]]:
        """Get application summary by ID - SLO: p99 < 50ms"""
        # Check cache first
        if application_id in self._cache:
            return self._cache[application_id]
        
        row = await self._fetchrow("""
            SELECT * FROM application_summary
            WHERE application_id = $1
        """, application_id)
        
        if row:
            result = dict(row)
            self._cache[application_id] = result
            return result
        
        return None
    
    async def get_all_applications(
        self, 
        limit: int = 100, 
        offset: int = 0,
        state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all applications with optional filtering"""
        query = "SELECT * FROM application_summary"
        params = []
        
        if state:
            query += " WHERE current_state = $1"
            params.append(state)
        
        query += " ORDER BY updated_at DESC LIMIT $2 OFFSET $3"
        params.extend([limit, offset])
        
        rows = await self._fetch(query, *params)
        return [dict(row) for row in rows]
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch"""
        await self._execute("TRUNCATE TABLE application_summary")
        self._cache.clear()
        
        async for batch in store.load_all():
            for event in batch:
                await self.handle_event(event)