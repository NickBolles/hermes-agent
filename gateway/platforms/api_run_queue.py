"""Bounded process-local FIFO admission for detached API runs.

Only the event-loop thread may call these methods. Waiting entries are not worker
slots; callers must keep them in their shutdown/drain inventory separately.
"""
import asyncio
from collections import OrderedDict


class RunQueue:
    def __init__(self, capacity=100):
        self.capacity = capacity
        self.waiting = OrderedDict()

    def reserve(self, run_id):
        # No await between the capacity check and insertion.
        if len(self.waiting) >= self.capacity:
            return False
        self.waiting[run_id] = None
        return True

    def discard(self, run_id):
        self.waiting.pop(run_id, None)

    async def acquire(self, run_id, available, cancelled):
        try:
            while run_id in self.waiting:
                if cancelled():
                    return False
                if next(iter(self.waiting)) == run_id and available():
                    # Removal and the caller's worker bookkeeping happen in the
                    # same event-loop tick, before another waiter can acquire.
                    self.discard(run_id)
                    return True
                await asyncio.sleep(0.05)
            return False
        finally:
            self.discard(run_id)
