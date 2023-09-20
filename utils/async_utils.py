"""
Async func helpers
"""

import asyncio
from functools import wraps, partial
from typing import Any, Coroutine, List


def run_async_tasks(tasks: List[Coroutine]) -> List[Any]:
    """Run a list of async tasks."""
    async def _gather() -> List[Any]:
        return await asyncio.gather(*tasks)
    outputs: List[Any] = asyncio.run(_gather())
    return outputs


def to_async(func):
    @wraps(func)  # Makes sure that function is returned for e.g. func.__name__ etc.
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop() # Make event loop of nothing exists
        pfunc = partial(func, *args, **kwargs)  # Return function with variables (event) filled in
        return await loop.run_in_executor(executor, pfunc)
    return run


