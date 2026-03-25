# src/what_if/projector.py
from typing import List, Dict, Any, Optional
from src.event_store import EventStore
from src.models.events import BaseEvent, StoredEvent
from src.aggregates.loan_application import LoanApplicationAggregate

class WhatIfProjector:
    """
    What-If projector for counterfactual scenarios.
    Never writes to the real store.
    """
    
    def __init__(self, store: EventStore):
        self.store = store
    
    async def run_what_if(
        self,
        application_id: str,
        branch_at_event_type: str,
        counterfactual_events: List[BaseEvent],
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run a what-if scenario by injecting counterfactual events.
        
        Args:
            application_id: The application to analyze
            branch_at_event_type: Event type to branch at
            counterfactual_events: Events to inject instead of real ones
            correlation_id: Optional correlation ID
            
        Returns:
            Dict with real_outcome, counterfactual_outcome, and divergence_events
        """
        # Load all real events
        stream_id = f"loan-{application_id}"
        real_events = await self.store.load_stream(stream_id)
        
        # Find branch point
        branch_index = -1
        for i, event in enumerate(real_events):
            if event.event_type == branch_at_event_type:
                branch_index = i
                break
        
        if branch_index == -1:
            raise ValueError(f"Event type {branch_at_event_type} not found in stream")
        
        branch_event = real_events[branch_index]
        
        # Identify causal dependencies
        dependent_indices = await self._find_causal_dependencies(
            real_events, 
            branch_index,
            branch_event.causation_id or branch_event.event_id
        )
        
        # Build counterfactual event list
        counterfactual_events_list = []
        # Add pre-branch real events
        for i, event in enumerate(real_events[:branch_index]):
            counterfactual_events_list.append(event)
        
        # Add counterfactual events
        for ce in counterfactual_events:
            counterfactual_events_list.append(ce)
        
        # Add post-branch independent events
        for i, event in enumerate(real_events[branch_index + 1:], start=branch_index + 1):
            if i not in dependent_indices:
                counterfactual_events_list.append(event)
        
        # Replay counterfactual events to compute outcome
        counterfactual_outcome = await self._replay_events(
            counterfactual_events_list,
            application_id
        )
        
        # Replay real events to get real outcome
        real_outcome = await self._replay_events(real_events, application_id)
        
        # Identify divergent events
        divergence_events = []
        for i in dependent_indices:
            if i < len(real_events):
                divergence_events.append({
                    "event_type": real_events[i].event_type,
                    "position": real_events[i].stream_position,
                    "reason": "Causally dependent on branched event"
                })
        
        return {
            "real_outcome": real_outcome,
            "counterfactual_outcome": counterfactual_outcome,
            "divergence_events": divergence_events
        }
    
    async def _find_causal_dependencies(
        self,
        events: List[StoredEvent],
        branch_index: int,
        branch_causation_id: Any
    ) -> List[int]:
        """
        Find indices of events causally dependent on the branch point.
        """
        dependent_indices = []
        
        for i, event in enumerate(events):
            if i <= branch_index:
                continue
            
            # Check if event's causation_id traces back to branch
            if event.causation_id == branch_causation_id:
                dependent_indices.append(i)
                # Recursively find dependencies of this event
                deps = await self._find_causal_dependencies(
                    events, i, event.event_id
                )
                dependent_indices.extend(deps)
        
        return list(set(dependent_indices))
    
    async def _replay_events(
        self,
        events: List[StoredEvent],
        application_id: str
    ) -> Dict[str, Any]:
        """
        Replay a list of events to compute aggregate state.
        """
        agg = LoanApplicationAggregate(application_id)
        
        for event in events:
            agg.apply_event(event)
        
        return {
            "application_id": application_id,
            "state": agg.state.value,
            "final_decision": agg.final_decision,
            "approved_amount": agg.approved_amount,
            "version": agg.version
        }