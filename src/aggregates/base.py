# src/aggregates/base.py
"""
Base aggregate class for all domain aggregates.
"""

from typing import Any, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import logging

from ..models.events import BaseEvent, StoredEvent
from ..models.errors import DomainError, InvalidEventError

logger = logging.getLogger(__name__)

T = TypeVar('T', bound='BaseAggregate')


class BaseAggregate(ABC):
    """
    Base class for all aggregates.
    
    Provides:
    - Event replay functionality
    - Version tracking
    - Apply pattern implementation
    - State validation
    """
    
    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.version = 0
        self.uncommitted_events: List[BaseEvent] = []
        self._initialized = False
    
    @classmethod
    async def load(
        cls,
        event_store,
        aggregate_id: str,
        **kwargs
    ) -> 'BaseAggregate':
        """
        Load aggregate from event store by replaying events.
        
        Args:
            event_store: EventStore instance
            aggregate_id: ID of the aggregate to load
            
        Returns:
            Reconstructed aggregate instance
        """
        stream_id = cls._get_stream_id(aggregate_id, **kwargs)
        stored_events = await event_store.load_stream(stream_id)
        
        if not stored_events:
            # Return new aggregate if no events exist
            return cls(aggregate_id, **kwargs)
        
        aggregate = cls(aggregate_id, **kwargs)
        
        for stored_event in stored_events:
            aggregate._apply_stored(stored_event)
        
        aggregate._initialized = True
        return aggregate
    
    @classmethod
    def _get_stream_id(cls, aggregate_id: str, **kwargs) -> str:
        """Get the stream ID for this aggregate type."""
        return f"{cls.__name__.lower()}-{aggregate_id}"
    
    def _apply_stored(self, stored_event: StoredEvent) -> None:
        """Apply a stored event to rebuild state."""
        # Convert stored event back to domain event
        event_class = self._get_event_class(stored_event.event_type)
        if event_class:
            domain_event = event_class.model_validate(stored_event.payload)
            self._apply(domain_event)
            self.version = stored_event.stream_position
    
    def _apply(self, event: BaseEvent) -> None:
        """
        Apply a domain event to the aggregate.
        Override in child classes to handle specific events.
        """
        handler_name = f"apply_{event.event_type.value}"
        handler = getattr(self, handler_name, None)
        
        if handler:
            handler(event)
        else:
            # Default handling - store that event was applied
            logger.debug(f"No handler for {event.event_type} on {self.__class__.__name__}")
    
    def add_event(self, event: BaseEvent) -> None:
        """
        Add an event to the uncommitted list.
        
        Args:
            event: Domain event to add
        """
        # Apply event to update state immediately
        self._apply(event)
        self.uncommitted_events.append(event)
    
    def clear_events(self) -> List[BaseEvent]:
        """Clear and return uncommitted events."""
        events = self.uncommitted_events
        self.uncommitted_events = []
        return events
    
    @abstractmethod
    def _get_event_class(self, event_type: str):
        """Get the event class for a given event type string."""
        pass
    
    def validate(self) -> None:
        """
        Validate aggregate state.
        Override to implement business rule validation.
        """
        pass
    
    def _raise_domain_error(self, message: str, details: Optional[Dict] = None) -> None:
        """Raise a domain error with consistent formatting."""
        raise DomainError(message, details)