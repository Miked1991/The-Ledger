# src/projections/daemon.py
"""
Async Projection Daemon - Fault-tolerant background processor for async projections.
Implements checkpoint management, retry logic, and lag monitoring.
"""

import asyncio
import logging
from typing import List, Dict, Optional, Any, Set
from datetime import datetime
from src.event_store import EventStore
from src.models.events import StoredEvent
from src.projections.base import Projection, AsyncProjection

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Async daemon that processes events and updates async projections.
    Implements fault-tolerant batch processing with checkpoint management.
    
    Features:
    - Checkpoint persistence for crash recovery
    - Configurable retry with exponential backoff
    - Lag monitoring for SLO compliance
    - Batch processing for efficiency
    - Dead letter queue for failed events
    """
    
    def __init__(
        self, 
        store: EventStore, 
        projections: List[Projection], 
        max_retries: int = 3,
        batch_size: int = 100,
        poll_interval_ms: int = 100,
        dead_letter_queue_enabled: bool = True
    ):
        self.store = store
        self._projections = {p.name: p for p in projections}
        self._async_projections = {
            name: p for name, p in self._projections.items() 
            if isinstance(p, AsyncProjection)
        }
        self._running = False
        self._checkpoints: Dict[str, int] = {}
        self._max_retries = max_retries
        self._batch_size = batch_size
        self._poll_interval_ms = poll_interval_ms
        self._dead_letter_queue_enabled = dead_letter_queue_enabled
        self._dead_letter_queue: List[Dict[str, Any]] = []
        self._stats = {
            "total_processed": 0,
            "total_errors": 0,
            "last_processed_at": None,
            "last_error_at": None
        }
    
    async def run_forever(self) -> None:
        """Run the daemon continuously"""
        self._running = True
        logger.info(f"Projection daemon started with {len(self._async_projections)} async projections")
        
        # Load initial checkpoints
        await self._load_checkpoints()
        
        while self._running:
            try:
                await self._process_batch()
            except Exception as e:
                logger.error(f"Error processing batch: {e}", exc_info=True)
                self._stats["total_errors"] += 1
                self._stats["last_error_at"] = datetime.utcnow()
            
            await asyncio.sleep(self._poll_interval_ms / 1000)
        
        logger.info("Projection daemon stopped")
    
    async def _load_checkpoints(self) -> None:
        """Load checkpoints from database"""
        async with self.store.pool.acquire() as conn:
            for projection_name in self._async_projections:
                row = await conn.fetchrow("""
                    SELECT last_position FROM projection_checkpoints
                    WHERE projection_name = $1
                """, projection_name)
                
                self._checkpoints[projection_name] = row['last_position'] if row else 0
                logger.debug(f"Loaded checkpoint for {projection_name}: {self._checkpoints[projection_name]}")
    
    async def _save_checkpoint(self, projection_name: str, position: int) -> None:
        """Save checkpoint to database"""
        async with self.store.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name) 
                DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()
            """, projection_name, position)
        
        self._checkpoints[projection_name] = position
        logger.debug(f"Saved checkpoint for {projection_name}: {position}")
    
    async def _process_batch(self) -> None:
        """Process a batch of events for all async projections"""
        if not self._async_projections:
            return
        
        # Find minimum checkpoint across all async projections
        min_position = min(self._checkpoints.values())
        
        # Get current global position
        async with self.store.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT MAX(global_position) FROM events")
            current_position = row[0] or 0
        
        if min_position >= current_position:
            # No new events
            return
        
        # Load events from min_position
        events_batch = []
        async for batch in self.store.load_all(
            from_global_position=min_position,
            batch_size=self._batch_size
        ):
            events_batch = batch
            break
        
        if not events_batch:
            return
        
        logger.debug(f"Processing batch of {len(events_batch)} events from position {min_position}")
        
        # Process each event through all async projections
        for projection_name, projection in self._async_projections.items():
            checkpoint = self._checkpoints[projection_name]
            
            # Filter events after checkpoint for this projection
            events_to_process = [
                e for e in events_batch 
                if e.global_position > checkpoint
            ]
            
            if not events_to_process:
                continue
            
            await self._process_events_for_projection(
                projection_name, 
                projection, 
                events_to_process
            )
            
            # Update checkpoint to last processed event
            last_position = events_to_process[-1].global_position
            await self._save_checkpoint(projection_name, last_position)
        
        # Update stats
        self._stats["total_processed"] += len(events_batch)
        self._stats["last_processed_at"] = datetime.utcnow()
        
        # Update lag metrics
        for projection_name, projection in self._async_projections.items():
            await projection.update_lag(current_position)
    
    async def _process_events_for_projection(
        self, 
        projection_name: str, 
        projection: AsyncProjection,
        events: List[StoredEvent]
    ) -> None:
        """Process a batch of events for a projection with retries"""
        for attempt in range(self._max_retries):
            try:
                await projection.handle_batch(events)
                return
            except Exception as e:
                logger.warning(
                    f"Error processing {len(events)} events for projection {projection_name}, "
                    f"attempt {attempt + 1}/{self._max_retries}: {e}"
                )
                
                if attempt == self._max_retries - 1:
                    # Final attempt failed
                    logger.error(
                        f"Failed to process {len(events)} events for projection {projection_name} "
                        f"after {self._max_retries} attempts"
                    )
                    
                    if self._dead_letter_queue_enabled:
                        # Send to dead letter queue
                        self._dead_letter_queue.append({
                            "projection": projection_name,
                            "events": [e.dict() for e in events],
                            "error": str(e),
                            "timestamp": datetime.utcnow().isoformat()
                        })
                        logger.warning(f"Added {len(events)} events to dead letter queue")
                else:
                    # Exponential backoff with jitter
                    delay = (0.1 * (2 ** attempt)) + (0.01 * attempt)
                    await asyncio.sleep(delay)
    
    async def rebuild_projection(self, projection_name: str) -> None:
        """Rebuild a specific projection from scratch"""
        if projection_name not in self._async_projections:
            raise ValueError(f"Unknown projection: {projection_name}")
        
        projection = self._async_projections[projection_name]
        logger.info(f"Rebuilding projection {projection_name}")
        
        await projection.rebuild(self.store)
        await self._save_checkpoint(projection_name, await self._get_current_global_position())
        
        logger.info(f"Rebuilt projection {projection_name}")
    
    async def rebuild_all_projections(self) -> None:
        """Rebuild all async projections from scratch"""
        for projection_name in self._async_projections:
            await self.rebuild_projection(projection_name)
    
    async def get_lag(self, projection_name: str) -> Dict[str, Any]:
        """Get lag metrics for a specific projection"""
        if projection_name not in self._async_projections:
            return {"error": f"Unknown projection: {projection_name}"}
        
        projection = self._async_projections[projection_name]
        return projection.get_lag()
    
    async def get_all_lags(self) -> Dict[str, Any]:
        """Get lag metrics for all projections"""
        current_position = await self._get_current_global_position()
        lags = {}
        
        for name, projection in self._async_projections.items():
            await projection.update_lag(current_position)
            lags[name] = projection.get_lag()
        
        return lags
    
    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get the dead letter queue contents"""
        return self._dead_letter_queue
    
    async def clear_dead_letter_queue(self) -> None:
        """Clear the dead letter queue"""
        self._dead_letter_queue = []
        logger.info("Dead letter queue cleared")
    
    async def reprocess_dead_letter(self) -> None:
        """Reprocess events from dead letter queue"""
        if not self._dead_letter_queue:
            return
        
        logger.info(f"Reprocessing {len(self._dead_letter_queue)} dead letter entries")
        
        for entry in self._dead_letter_queue:
            projection = self._async_projections.get(entry["projection"])
            if projection:
                events = [StoredEvent(**e) for e in entry["events"]]
                try:
                    await projection.handle_batch(events)
                    logger.info(f"Reprocessed {len(events)} events for {entry['projection']}")
                except Exception as e:
                    logger.error(f"Failed to reprocess events: {e}")
        
        self._dead_letter_queue = []
    
    async def _get_current_global_position(self) -> int:
        """Get current global position from events table"""
        async with self.store.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT MAX(global_position) FROM events")
            return row[0] or 0
    
    def stop(self) -> None:
        """Stop the daemon"""
        self._running = False
        logger.info("Projection daemon stop requested")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get daemon statistics"""
        return {
            **self._stats,
            "running": self._running,
            "projections": len(self._async_projections),
            "checkpoints": self._checkpoints,
            "dead_letter_size": len(self._dead_letter_queue)
        }