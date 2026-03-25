# src/upcasting/registry.py
"""
Upcaster registry for event schema evolution.
"""

from typing import Dict, Callable, Tuple, Any
import logging
from functools import wraps

from ..models.events import StoredEvent

logger = logging.getLogger(__name__)


class UpcasterRegistry:
    """
    Registry for event upcasters that handle schema evolution.
    
    Upcasters transform old event versions to newer versions at read time,
    preserving the immutable stored events.
    """
    
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable] = {}
    
    def register(self, event_type: str, from_version: int):
        """
        Decorator to register an upcaster function.
        
        Args:
            event_type: Name of the event type
            from_version: Version to upcast from
            
        Returns:
            Decorator function
        """
        def decorator(fn: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            @wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)
            return wrapper
        return decorator
    
    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply all registered upcasters to an event in order.
        
        Args:
            event: Stored event to upcast
            
        Returns:
            Upcasted event (if any upcasters applied)
        """
        current = event
        current_version = event.event_version
        current_payload = event.payload
        
        # Apply upcasters sequentially until no more found
        while True:
            key = (event.event_type, current_version)
            if key not in self._upcasters:
                break
            
            upcaster = self._upcasters[key]
            try:
                new_payload = upcaster(current_payload)
                current_version += 1
                current = current.with_payload(new_payload, current_version)
                current_payload = new_payload
                logger.debug(f"Upcast {event.event_type} v{event.event_version} -> v{current_version}")
            except Exception as e:
                logger.error(f"Upcaster failed for {event.event_type} v{current_version}: {e}")
                raise
        
        return current