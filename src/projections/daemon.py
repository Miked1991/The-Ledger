# src/projections/daemon.py
"""
Async projection daemon for building read models.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncpg

from ..event_store import EventStore
from .base import Projection
from ..models.errors import ProjectionError

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Async daemon that processes events and updates projections.
    
    Features:
    - Fault-tolerant batch processing
    - Per-projection checkpoint management
    - Configurable retry with backoff
    - Lag metrics exposure
    """
    
    def __init__(
        self,
        store: EventStore,
        projections: List[Projection],
        batch_size: int = 100,
        max_retries: int = 3,
        retry_delay_ms: int = 1000
    ):
        """
        Initialize projection daemon.
        
        Args:
            store: Event store instance
            projections: List of projections to maintain
            batch_size: Number of events to process per batch
            max_retries: Maximum retry attempts for failed events
            retry_delay_ms: Delay between retries
        """
        self.store = store
        self.projections = {p.name: p for p in projections}
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay_ms / 1000
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._checkpoints: Dict[str, int] = {}
        self._failed_events: Dict[str, Dict[str, int]] = {}  # projection_name -> event_id -> retry_count
    
    async def start(self):
        """Start the daemon."""
        if self._running:
            return
        
        # Load checkpoints
        await self._load_checkpoints()
        
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info("Projection daemon started")
    
    async def stop(self):
        """Stop the daemon gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Projection daemon stopped")
    
    async def _load_checkpoints(self):
        """Load checkpoints from database."""
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT projection_name, last_position FROM projection_checkpoints"
            )
            for row in rows:
                self._checkpoints[row["projection_name"]] = row["last_position"]
    
    async def _update_checkpoint(self, projection_name: str, position: int):
        """Update checkpoint for a projection."""
        async with self.store.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (projection_name)
                DO UPDATE SET last_position = $2, updated_at = NOW()
            """, projection_name, position)
        
        self._checkpoints[projection_name] = position
    
    async def _run_forever(self):
        """Main daemon loop."""
        while self._running:
            try:
                await self._process_batch()
                await asyncio.sleep(0.1)  # 100ms polling interval
            except Exception as e:
                logger.error(f"Error in projection daemon: {e}")
                await asyncio.sleep(1)
    
    async def _process_batch(self):
        """Process a batch of events."""
        # Get lowest checkpoint across all projections
        min_position = min(self._checkpoints.values()) if self._checkpoints else 0
        
        # Get current global position
        current_global = await self.store.get_global_position()
        
        if min_position >= current_global:
            return
        
        # Load events from min_position
        async for batch in self.store.load_all(
            from_global_position=min_position + 1,
            batch_size=self.batch_size
        ):
            # Process each event
            for event in batch:
                for projection_name, projection in self.projections.items():
                    if projection.should_process(event):
                        await self._process_event_for_projection(
                            projection_name,
                            projection,
                            event
                        )
            
            # Update checkpoints after batch
            if batch:
                last_position = batch[-1].global_position
                for projection_name in self.projections:
                    await self._update_checkpoint(projection_name, last_position)
    
    async def _process_event_for_projection(
        self,
        projection_name: str,
        projection: Projection,
        event
    ):
        """Process an event for a specific projection with retry logic."""
        event_id = str(event.event_id)
        retry_key = f"{projection_name}:{event_id}"
        
        for attempt in range(self.max_retries):
            try:
                await projection.process(event)
                return
            except Exception as e:
                logger.warning(
                    f"Failed to process event {event_id} for projection {projection_name}: {e}"
                )
                
                if attempt == self.max_retries - 1:
                    # Last attempt failed
                    logger.error(
                        f"Permanent failure for event {event_id} in projection {projection_name}"
                    )
                    raise ProjectionError(projection_name, event_id, str(e))
                
                await asyncio.sleep(self.retry_delay * (attempt + 1))
    
    def get_lag(self) -> Dict[str, int]:
        """
        Get lag for each projection.
        
        Returns:
            Dict mapping projection name to lag (number of events behind)
        """
        current_global = asyncio.run_coroutine_threadsafe(
            self.store.get_global_position(),
            asyncio.get_event_loop()
        ).result()
        
        lags = {}
        for name, checkpoint in self._checkpoints.items():
            lags[name] = current_global - checkpoint
        
        return lags
    
    async def rebuild_projection(self, projection_name: str):
        """
        Rebuild a projection from scratch by replaying all events.
        
        Args:
            projection_name: Name of projection to rebuild
        """
        if projection_name not in self.projections:
            raise ValueError(f"Unknown projection: {projection_name}")
        
        logger.info(f"Rebuilding projection {projection_name}")
        
        # Clear existing data for this projection
        projection = self.projections[projection_name]
        await projection.clear()
        
        # Reset checkpoint
        await self._update_checkpoint(projection_name, 0)
        
        # Replay all events
        async for batch in self.store.load_all(batch_size=1000):
            for event in batch:
                if projection.should_process(event):
                    try:
                        await projection.process(event)
                    except Exception as e:
                        logger.error(f"Error rebuilding projection {projection_name}: {e}")
                        raise
        
        logger.info(f"Rebuilt projection {projection_name}")