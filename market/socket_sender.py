"""One bounded FIFO sender per socket; producers never wait for the network."""

import asyncio
from contextlib import suppress


class SocketSender:
    def __init__(self, websocket, on_close, *, capacity=256, timeout=2.0):
        self.websocket = websocket
        self.on_close = on_close
        self.timeout = timeout
        self.queue = asyncio.Queue(maxsize=capacity)
        self.closed = False
        self.task = asyncio.create_task(self._run(), name="socket_sender")

    def send(self, message: str) -> None:
        if self.closed:
            return
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            # Never silently skip an internal risk tick. Disconnect so the
            # consumer can detect a gap, instead of growing an endless backlog.
            self.stop()

    def stop(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(None)

    async def _run(self) -> None:
        try:
            while not self.closed:
                message = await self.queue.get()
                if self.closed:
                    break
                await asyncio.wait_for(self.websocket.send_text(message), self.timeout)
        except (asyncio.CancelledError, Exception):
            pass
        finally:
            self.closed = True
            self.on_close(self)
            with suppress(Exception, asyncio.CancelledError):
                await asyncio.wait_for(self.websocket.close(code=1013), self.timeout)
