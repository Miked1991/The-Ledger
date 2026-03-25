# src/event_store.py - Add stats and monitoring
"""
Complete Event Store implementation with monitoring and statistics.
"""

import asyncio
import asyncpg
from typing import List, Optional, Dict, Any, AsyncIterator, Tuple
from uuid import UUID, uuid4
from datetime import datetime
import json
import logging
import time

from src.models.events import StoredEvent, StreamMetadata, BaseEvent
from src.models.errors import (
    OptimisticConcurrencyError, 
    StreamNotFoundError,
    EventStoreError
)
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)


class EventStore:
    """
    Core event store implementation with PostgreSQL backend.
    Implements event sourcing with optimistic concurrency control,
    outbox pattern, and automatic upcasting.
    
    SLO Commitments:
    - Write latency: p99 < 100ms
    - Read latency (load_stream): p99 < 50ms for 100 events
    - Read latency (load_all): p99 < 500ms for 1000 events
    """
    
    def __init__(
        self, 
        pool: asyncpg.Pool, 
        upcaster_registry: Optional[UpcasterRegistry] = None
    ):
        self.pool = pool
        self.upcaster_registry = upcaster_registry or UpcasterRegistry()
        
        # Statistics
        self._stats = {
            "total_appends": 0,
            "successful_appends": 0,
            "failed_appends": 0,
            "total_loads": 0,
            "total_events_read": 0,
            "concurrency_conflicts": 0,
            "append_latencies_ms": [],
            "load_latencies_ms": []
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event store statistics"""
        return {
            **self._stats,
            "append_success_rate": (
                self._stats["successful_appends"] / self._stats["total_appends"] * 100
                if self._stats["total_appends"] > 0 else 0
            ),
            "avg_append_latency_ms": (
                sum(self._stats["append_latencies_ms"]) / len(self._stats["append_latencies_ms"])
                if self._stats["append_latencies_ms"] else 0
            ),
            "p99_append_latency_ms": self._calculate_p99(self._stats["append_latencies_ms"])
        }
    
    def _calculate_p99(self, latencies: List[float]) -> float:
        """Calculate p99 latency"""
        if not latencies:
            return 0
        sorted_latencies = sorted(latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[idx] if idx < len(sorted_latencies) else sorted_latencies[-1]
    
    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> int:
        """
        Append events to a stream with optimistic concurrency control.
        
        SLO: p99 < 100ms
        """
        start_time = time.time()
        self._stats["total_appends"] += 1
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Get current stream version
                    current_version = await self._get_stream_version(conn, stream_id)
                    
                    # Handle new stream case
                    if expected_version == -1:
                        if current_version is not None:
                            self._stats["concurrency_conflicts"] += 1
                            raise OptimisticConcurrencyError(
                                stream_id, expected_version, current_version,
                                "Stream already exists"
                            )
                        current_version = 0
                    else:
                        if current_version is None:
                            raise StreamNotFoundError(stream_id)
                        if current_version != expected_version:
                            self._stats["concurrency_conflicts"] += 1
                            raise OptimisticConcurrencyError(
                                stream_id, expected_version, current_version
                            )
                    
                    # Build events to insert
                    events_to_insert = []
                    for i, event in enumerate(events):
                        stream_position = current_version + i + 1
                        
                        events_to_insert.append((
                            stream_id,
                            stream_position,
                            event.event_type,
                            event.event_version,
                            json.dumps(event.to_dict()),
                            json.dumps(event.model_dump().get('metadata', {})),
                            correlation_id,
                            causation_id,
                        ))
                    
                    # Insert events
                    insert_query = """
                        INSERT INTO events (
                            stream_id, stream_position, event_type, event_version,
                            payload, metadata, correlation_id, causation_id
                        ) SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[], 
                            $4::smallint[], $5::jsonb[], $6::jsonb[], $7::text[], $8::text[])
                        RETURNING event_id, global_position
                    """
                    rows = await conn.fetch(insert_query, *zip(*events_to_insert))
                    
                    # Update stream metadata
                    if current_version == 0:
                        await conn.execute("""
                            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                            VALUES ($1, $2, $3)
                        """, stream_id, events[0].event_type.split('.')[0] if '.' in events[0].event_type else events[0].event_type,
                           current_version + len(events))
                    else:
                        await conn.execute("""
                            UPDATE event_streams 
                            SET current_version = current_version + $1
                            WHERE stream_id = $2
                        """, len(events), stream_id)
                    
                    # Write to outbox
                    outbox_records = []
                    for (event_id, global_pos), event in zip(rows, events):
                        outbox_records.append((
                            event_id,
                            "event_bus",
                            json.dumps({
                                "event_type": event.event_type,
                                "event_version": event.event_version,
                                "payload": event.to_dict(),
                                "stream_id": stream_id,
                                "global_position": global_pos,
                                "correlation_id": correlation_id,
                                "causation_id": causation_id,
                            })
                        ))
                    
                    if outbox_records:
                        await conn.executemany("""
                            INSERT INTO outbox (event_id, destination, payload)
                            VALUES ($1, $2, $3)
                        """, outbox_records)
                    
                    self._stats["successful_appends"] += 1
                    return current_version + len(events)
                    
        except Exception as e:
            self._stats["failed_appends"] += 1
            raise
        finally:
            latency_ms = (time.time() - start_time) * 1000
            self._stats["append_latencies_ms"].append(latency_ms)
            # Keep only last 1000 measurements
            if len(self._stats["append_latencies_ms"]) > 1000:
                self._stats["append_latencies_ms"] = self._stats["append_latencies_ms"][-1000:]
    
    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: Optional[int] = None,
    ) -> List[StoredEvent]:
        """
        Load events from a stream with automatic upcasting.
        
        SLO: p99 < 50ms for 100 events
        """
        start_time = time.time()
        self._stats["total_loads"] += 1
        
        try:
            query = """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata,
                       correlation_id, causation_id, recorded_at
                FROM events
                WHERE stream_id = $1 AND stream_position >= $2
            """
            params = [stream_id, from_position]
            
            if to_position is not None:
                query += " AND stream_position <= $3"
                params.append(to_position)
            
            query += " ORDER BY stream_position ASC"
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                
                stored_events = []
                for row in rows:
                    stored_event = StoredEvent(
                        event_id=row['event_id'],
                        stream_id=row['stream_id'],
                        stream_position=row['stream_position'],
                        global_position=row['global_position'],
                        event_type=row['event_type'],
                        event_version=row['event_version'],
                        payload=row['payload'],
                        metadata=row['metadata'],
                        correlation_id=row['correlation_id'],
                        causation_id=row['causation_id'],
                        recorded_at=row['recorded_at']
                    )
                    upcasted = self.upcaster_registry.upcast(stored_event)
                    stored_events.append(upcasted)
                
                self._stats["total_events_read"] += len(stored_events)
                return stored_events
                
        finally:
            latency_ms = (time.time() - start_time) * 1000
            self._stats["load_latencies_ms"].append(latency_ms)
            if len(self._stats["load_latencies_ms"]) > 1000:
                self._stats["load_latencies_ms"] = self._stats["load_latencies_ms"][-1000:]
    
    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: Optional[List[str]] = None,
        batch_size: int = 500
    ) -> AsyncIterator[List[StoredEvent]]:
        """
        Load all events from global position with optional filtering.
        
        SLO: p99 < 500ms per batch for 500 events
        """
        query = """
            SELECT event_id, stream_id, stream_position, global_position,
                   event_type, event_version, payload, metadata,
                   correlation_id, causation_id, recorded_at
            FROM events
            WHERE global_position > $1
        """
        params = [from_global_position]
        
        if event_types:
            query += " AND event_type = ANY($2)"
            params.append(event_types)
        
        query += " ORDER BY global_position ASC LIMIT $3"
        
        last_position = from_global_position
        
        while True:
            params[-1] = batch_size
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                
                if not rows:
                    break
                
                batch = []
                for row in rows:
                    stored_event = StoredEvent(
                        event_id=row['event_id'],
                        stream_id=row['stream_id'],
                        stream_position=row['stream_position'],
                        global_position=row['global_position'],
                        event_type=row['event_type'],
                        event_version=row['event_version'],
                        payload=row['payload'],
                        metadata=row['metadata'],
                        correlation_id=row['correlation_id'],
                        causation_id=row['causation_id'],
                        recorded_at=row['recorded_at']
                    )
                    upcasted = self.upcaster_registry.upcast(stored_event)
                    batch.append(upcasted)
                    last_position = row['global_position']
                
                self._stats["total_events_read"] += len(batch)
                yield batch
                
                params[0] = last_position
    
    async def stream_version(self, stream_id: str) -> int:
        """Get the current version of a stream"""
        async with self.pool.acquire() as conn:
            version = await self._get_stream_version(conn, stream_id)
            return version if version is not None else 0
    
    async def archive_stream(self, stream_id: str) -> None:
        """Archive a stream (soft delete)"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE event_streams 
                SET archived_at = NOW()
                WHERE stream_id = $1
            """, stream_id)
    
    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """Get metadata for a stream"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT stream_id, aggregate_type, current_version, 
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
            """, stream_id)
            
            if not row:
                raise StreamNotFoundError(stream_id)
            
            return StreamMetadata(
                stream_id=row['stream_id'],
                aggregate_type=row['aggregate_type'],
                current_version=row['current_version'],
                created_at=row['created_at'],
                archived_at=row['archived_at'],
                metadata=row['metadata']
            )
    
    async def _get_stream_version(self, conn: asyncpg.Connection, stream_id: str) -> Optional[int]:
        """Get current stream version from database"""
        row = await conn.fetchrow("""
            SELECT current_version FROM event_streams WHERE stream_id = $1
        """, stream_id)
        return row['current_version'] if row else None