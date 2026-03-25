# src/projections/base.py
"""
Base classes for all projections in the Ledger system.
Implements both inline and async projection patterns for CQRS.
"""

from typing import Dict, Any, Optional, List, Callable, Awaitable
from abc import ABC, abstractmethod
import asyncpg
import logging
from datetime import datetime
from src.event_store import EventStore
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class Projection(ABC):
    """Base class for all projections"""
    
    def __init__(self, name: str):
        self.name = name
        self._pool: Optional[asyncpg.Pool] = None
        self._store: Optional[EventStore] = None
        self._last_processed_position: int = 0
        self._error_count: int = 0
        self._last_error: Optional[str] = None
        self._processed_count: int = 0
    
    @abstractmethod
    async def handle_event(self, event: StoredEvent) -> None:
        """Handle a single event and update projection"""
        pass
    
    @abstractmethod
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch"""
        pass
    
    def set_pool(self, pool: asyncpg.Pool) -> None:
        """Set database connection pool"""
        self._pool = pool
    
    def set_store(self, store: EventStore) -> None:
        """Set event store reference"""
        self._store = store
    
    async def _execute(self, query: str, *args) -> None:
        """Execute a query with the pool"""
        if not self._pool:
            raise RuntimeError(f"Projection {self.name} has no database pool")
        await self._pool.execute(query, *args)
    
    async def _fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Fetch rows from the database"""
        if not self._pool:
            raise RuntimeError(f"Projection {self.name} has no database pool")
        return await self._pool.fetch(query, *args)
    
    async def _fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch a single row from the database"""
        if not self._pool:
            raise RuntimeError(f"Projection {self.name} has no database pool")
        return await self._pool.fetchrow(query, *args)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get projection statistics"""
        return {
            "name": self.name,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "last_position": self._last_processed_position
        }


class InlineProjection(Projection):
    """
    Inline projection - updated synchronously in the same transaction as events.
    Provides strong consistency but higher write latency.
    """
    
    def __init__(self, name: str):
        super().__init__(name)
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Handle event - will be called within transaction"""
        try:
            await self._handle_event_in_transaction(event)
            self._processed_count += 1
            self._last_processed_position = event.global_position
        except Exception as e:
            self._error_count += 1
            self._last_error = str(e)
            logger.error(f"Inline projection {self.name} failed: {e}", exc_info=True)
            raise
    
    @abstractmethod
    async def _handle_event_in_transaction(self, event: StoredEvent) -> None:
        """Handle event within the same transaction as event append"""
        pass


class AsyncProjection(Projection):
    """
    Async projection - updated asynchronously via daemon.
    Provides eventual consistency with lower write latency.
    """
    
    def __init__(self, name: str, batch_size: int = 100):
        super().__init__(name)
        self.batch_size = batch_size
        self._last_checkpoint: int = 0
        self._lag_events: int = 0
        self._lag_ms: float = 0
    
    @abstractmethod
    async def handle_batch(self, events: List[StoredEvent]) -> None:
        """Handle a batch of events"""
        pass
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Single event handling - can be overridden for single event processing"""
        await self.handle_batch([event])
        self._processed_count += 1
        self._last_processed_position = event.global_position
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild by replaying all events"""
        start_time = datetime.utcnow()
        logger.info(f"Rebuilding projection {self.name}")
        
        # Clear existing data
        await self._clear()
        
        # Replay all events in batches
        event_count = 0
        async for batch in store.load_all(batch_size=self.batch_size):
            await self.handle_batch(batch)
            event_count += len(batch)
            self._last_processed_position = batch[-1].global_position if batch else 0
        
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Rebuilt projection {self.name}: {event_count} events in {elapsed:.2f}s")
    
    @abstractmethod
    async def _clear(self) -> None:
        """Clear all data in this projection"""
        pass
    
    async def update_lag(self, current_global_position: int) -> None:
        """Update lag metrics"""
        self._lag_events = current_global_position - self._last_processed_position
        # Estimate lag in ms (assuming 0.5ms per event processing)
        self._lag_ms = self._lag_events * 0.5
    
    def get_lag(self) -> Dict[str, Any]:
        """Get current lag metrics"""
        return {
            "events": self._lag_events,
            "estimated_ms": self._lag_ms,
            "last_position": self._last_processed_position
        }