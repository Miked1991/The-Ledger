"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [X] Phase 1, Day 1: append() + stream_version()
  [X] Phase 1, Day 1: load_stream()
  [X] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [X] Phase 1, Day 2: get_event() (needed for causation chain)
  [X] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import AsyncGenerator, Optional, List, Dict, Any
from uuid import UUID
import asyncpg


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    IMPLEMENT IN ORDER — see inline guides in each method:
      1. stream_version()   — simplest, needed immediately
      2. append()           — most critical; OCC correctness is the exam
      3. load_stream()      — needed for aggregate replay
      4. load_all()         — async generator, needed for projection daemon
      5. get_event()        — needed for causation chain audit
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Initialize connection pool"""
        self._pool = await asyncpg.create_pool(
            self.db_url, 
            min_size=2, 
            max_size=10,
            command_timeout=60
        )

    async def close(self) -> None:
        """Close connection pool"""
        if self._pool:
            await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or -1 if stream doesn't exist.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,    # -1=new stream, 0+=expected current
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Appends events atomically with OCC. Returns list of positions assigned.

        FULL IMPLEMENTATION — complete and uncommented for production:
        """
        if not events:
            raise ValueError("No events to append")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    # Extract aggregate type from stream_id (first part before hyphen)
                    aggregate_type = stream_id.split("-")[0] if "-" in stream_id else "unknown"
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version, created_at)"
                        " VALUES($1, $2, 0, $3)",
                        stream_id, aggregate_type, datetime.utcnow()
                    )

                # 4. Insert each event
                positions = []
                meta = {**(metadata or {})}
                if causation_id:
                    meta["causation_id"] = causation_id
                
                for i, event in enumerate(events):
                    pos = expected_version + 1 + i
                    
                    # Ensure event has required fields
                    if "event_type" not in event:
                        raise ValueError(f"Event missing 'event_type': {event}")
                    
                    event_version = event.get("event_version", 1)
                    payload = event.get("payload", {})
                    
                    # Insert event
                    await conn.execute(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)",
                        stream_id, pos,
                        event["event_type"], event_version,
                        json.dumps(payload),
                        json.dumps(meta),
                        datetime.utcnow()
                    )
                    positions.append(pos)

                # 5. Update stream version
                new_version = expected_version + len(events)
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    new_version, stream_id
                )
                
                return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        """
        Loads events from a stream in stream_position order.
        Applies upcasters if self.upcasters is set.
        """
        async with self._pool.acquire() as conn:
            # Build query
            q = ("SELECT event_id, stream_id, stream_position, global_position, event_type,"
                 " event_version, payload, metadata, recorded_at"
                 " FROM events WHERE stream_id=$1 AND stream_position>=$2")
            params = [stream_id, from_position]
            
            if to_position is not None:
                q += " AND stream_position<=$3"
                params.append(to_position)
            
            q += " ORDER BY stream_position ASC"
            
            rows = await conn.fetch(q, *params)
            
            events = []
            for row in rows:
                # Convert row to dict with parsed JSON
                e = {
                    "event_id": str(row["event_id"]),
                    "stream_id": row["stream_id"],
                    "stream_position": row["stream_position"],
                    "global_position": row["global_position"],
                    "event_type": row["event_type"],
                    "event_version": row["event_version"],
                    "payload": dict(row["payload"]),
                    "metadata": dict(row["metadata"]),
                    "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None
                }
                
                # Apply upcasting if registry exists
                if self.upcasters:
                    e = self.upcasters.upcast(e)
                
                events.append(e)
            
            return events

    async def load_all(
        self, from_position: int = 0, batch_size: int = 500
    ) -> AsyncGenerator[dict, None]:
        """
        Async generator yielding all events by global_position.
        Used by the ProjectionDaemon.
        """
        async with self._pool.acquire() as conn:
            pos = from_position
            while True:
                rows = await conn.fetch(
                    "SELECT global_position, stream_id, stream_position,"
                    " event_type, event_version, payload, metadata, recorded_at"
                    " FROM events WHERE global_position > $1"
                    " ORDER BY global_position ASC LIMIT $2",
                    pos, batch_size
                )
                
                if not rows:
                    break
                
                for row in rows:
                    e = {
                        "event_id": str(row["event_id"]) if "event_id" in row else None,
                        "stream_id": row["stream_id"],
                        "stream_position": row["stream_position"],
                        "global_position": row["global_position"],
                        "event_type": row["event_type"],
                        "event_version": row["event_version"],
                        "payload": dict(row["payload"]),
                        "metadata": dict(row["metadata"]),
                        "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None
                    }
                    
                    # Apply upcasting if registry exists
                    if self.upcasters:
                        e = self.upcasters.upcast(e)
                    
                    yield e
                
                pos = rows[-1]["global_position"]
                
                # If we got fewer than batch_size, this was the last batch
                if len(rows) < batch_size:
                    break

    async def get_event(self, event_id: UUID) -> dict | None:
        """
        Loads one event by UUID. Used for causation chain lookups.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, stream_id, stream_position, global_position,"
                " event_type, event_version, payload, metadata, recorded_at"
                " FROM events WHERE event_id=$1", 
                event_id
            )
            
            if not row:
                return None
            
            event = {
                "event_id": str(row["event_id"]),
                "stream_id": row["stream_id"],
                "stream_position": row["stream_position"],
                "global_position": row["global_position"],
                "event_type": row["event_type"],
                "event_version": row["event_version"],
                "payload": dict(row["payload"]),
                "metadata": dict(row["metadata"]),
                "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None
            }
            
            # Apply upcasting if registry exists
            if self.upcasters:
                event = self.upcasters.upcast(event)
            
            return event

    # Helper methods for projection daemon integration
    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        """Save projection checkpoint"""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name) DO UPDATE
                SET last_position = $2, updated_at = NOW()
            """, projection_name, position)

    async def load_checkpoint(self, projection_name: str) -> int:
        """Load projection checkpoint"""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                projection_name
            )
            return row["last_position"] if row else 0

    async def archive_stream(self, stream_id: str) -> None:
        """Archive a stream (mark as archived)"""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                UPDATE event_streams
                SET archived_at = NOW()
                WHERE stream_id = $1
            """, stream_id)

    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """Get metadata for a stream"""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT stream_id, aggregate_type, current_version,
                       created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
            """, stream_id)
            
            if not row:
                return None
            
            return {
                "stream_id": row["stream_id"],
                "aggregate_type": row["aggregate_type"],
                "current_version": row["current_version"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "archived_at": row["archived_at"].isoformat() if row["archived_at"] else None,
                "metadata": dict(row["metadata"]) if row["metadata"] else {}
            }


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            # v2 adds model_versions dict
            payload.setdefault("model_versions", {})
            return payload

    REQUIRED FOR PHASE 4:
        - CreditAnalysisCompleted  v1 → v2  (adds model_versions: dict)
        - DecisionGenerated        v1 → v2  (adds model_versions: dict)

    IMMUTABILITY TEST (required artifact):
        registry.assert_upcaster_does_not_write_to_db(store, event)
        # Loads the event, upcasts it, re-loads it, confirms DB row unchanged.
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        """Decorator to register an upcaster"""
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """Apply chain of upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        
        # Make a copy to avoid modifying the original
        result = event.copy()
        
        while v in chain:
            # Apply upcaster
            result["payload"] = chain[v](dict(result["payload"]))
            v += 1
            result["event_version"] = v
        
        return result

    def has_upcasters(self, event_type: str, version: int) -> bool:
        """Check if upcasters exist for this event type and version"""
        return event_type in self._upcasters and version in self._upcasters[event_type]


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for Phase 1 tests only
# Identical interface to EventStore. Drop-in for tests; never use in production.
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from uuid import uuid4 as _uuid4

class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store.
    Used exclusively in Phase 1 tests and conftest fixtures.
    Same interface as EventStore — swap one for the other with no code changes.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        # stream_id -> list of event dicts
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        # stream_id -> current version (position of last event, -1 if empty)
        self._versions: dict[str, int] = {}
        # global append log (ordered by insertion)
        self._global: list[dict] = []
        # projection checkpoints
        self._checkpoints: dict[str, int] = {}
        # asyncio lock per stream for OCC
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        if not events:
            raise ValueError("No events to append")
            
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            positions = []
            meta = {**(metadata or {})}
            if causation_id:
                meta["causation_id"] = causation_id

            for i, event in enumerate(events):
                pos = current + 1 + i
                stored = {
                    "event_id": str(_uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": _datetime.utcnow().isoformat(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = current + len(events)
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = [
            dict(e) for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
        ]
        events = sorted(events, key=lambda e: e["stream_position"])
        
        # Apply upcasting if registry exists
        if self.upcasters:
            events = [self.upcasters.upcast(e) for e in events]
            
        return events

    async def load_all(self, from_position: int = 0, batch_size: int = 500):
        """Async generator for all events"""
        for e in self._global:
            if e["global_position"] >= from_position:
                event = dict(e)
                if self.upcasters:
                    event = self.upcasters.upcast(event)
                yield event

    async def get_event(self, event_id: str) -> dict | None:
        for e in self._global:
            if e["event_id"] == event_id:
                event = dict(e)
                if self.upcasters:
                    event = self.upcasters.upcast(event)
                return event
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
        
    async def archive_stream(self, stream_id: str) -> None:
        """Archive a stream (mark as archived) - in-memory version"""
        # For in-memory, we just note it's archived
        pass
        
    async def get_stream_metadata(self, stream_id: str) -> dict | None:
        """Get stream metadata - in-memory version"""
        if stream_id not in self._versions:
            return None
        
        # Infer aggregate type from stream_id
        aggregate_type = stream_id.split("-")[0] if "-" in stream_id else "unknown"
        
        return {
            "stream_id": stream_id,
            "aggregate_type": aggregate_type,
            "current_version": self._versions.get(stream_id, -1),
            "created_at": _datetime.utcnow().isoformat(),
            "archived_at": None,
            "metadata": {}
        }