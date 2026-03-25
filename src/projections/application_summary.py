# src/projections/application_summary.py
from src.projections.daemon import Projection
from src.event_store import EventStore
from src.models.events import StoredEvent
from typing import Dict, Any
import asyncpg

class ApplicationSummaryProjection(Projection):
    """
    Application summary projection.
    Maintains current state of each loan application for fast queries.
    This is an inline projection (updated in same transaction as events).
    """
    
    def __init__(self):
        super().__init__("application_summary")
        self._pool: asyncpg.Pool = None
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Update projection based on event"""
        if not self._pool:
            return
        
        if event.event_type == "ApplicationSubmitted":
            await self._handle_submitted(event)
        elif event.event_type == "CreditAnalysisCompleted":
            await self._handle_credit_analysis(event)
        elif event.event_type == "DecisionGenerated":
            await self._handle_decision(event)
        elif event.event_type == "HumanReviewCompleted":
            await self._handle_human_review(event)
    
    async def _handle_submitted(self, event: StoredEvent) -> None:
        """Handle application submission"""
        application_id = event.payload["application_id"]
        
        await self._pool.execute("""
            INSERT INTO application_summary (
                application_id, applicant_id, requested_amount,
                current_state, updated_at
            ) VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (application_id) DO UPDATE SET
                current_state = EXCLUDED.current_state,
                updated_at = NOW()
        """, application_id, event.payload["applicant_id"],
           event.payload["requested_amount_usd"],
           "SUBMITTED")
    
    async def _handle_credit_analysis(self, event: StoredEvent) -> None:
        """Handle credit analysis completion"""
        await self._pool.execute("""
            UPDATE application_summary
            SET credit_analysis_tier = $1,
                current_state = 'ANALYSIS_COMPLETE',
                updated_at = NOW()
            WHERE application_id = $2
        """, event.payload["risk_tier"], event.payload["application_id"])
    
    async def _handle_decision(self, event: StoredEvent) -> None:
        """Handle decision generation"""
        await self._pool.execute("""
            UPDATE application_summary
            SET final_decision = $1,
                current_state = $2,
                updated_at = NOW()
            WHERE application_id = $3
        """, event.payload["recommendation"],
           "PENDING_DECISION" if event.payload["recommendation"] == "REFER" 
           else f"{event.payload['recommendation']}_PENDING_HUMAN",
           event.payload["application_id"])
    
    async def _handle_human_review(self, event: StoredEvent) -> None:
        """Handle human review completion"""
        await self._pool.execute("""
            UPDATE application_summary
            SET final_decision = $1,
                current_state = $2,
                human_reviewer_id = $3,
                updated_at = NOW()
            WHERE application_id = $4
        """, event.payload["final_decision"],
           "FINAL_APPROVED" if event.payload["final_decision"] == "APPROVED" else "FINAL_DECLINED",
           event.payload["reviewer_id"],
           event.payload["application_id"])
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch"""
        # Clear existing data
        await self._pool.execute("TRUNCATE TABLE application_summary")
        
        # Replay all events
        async for batch in store.load_all():
            for event in batch:
                await self.handle_event(event)
    
    def set_pool(self, pool: asyncpg.Pool) -> None:
        """Set database connection pool"""
        self._pool = pool