"""单线程 actor，串行所有 chan_core 操作。

chan_core 的 MultiChan / ChanEngine 在 pyo3 端标 `unsendable`：
实例只能在创建它的线程里用，跨线程访问会运行时报错。FastAPI 默认把
`def` 路由放到线程池里执行，意味着同一个 MultiChan 可能被多个线程接触。

解决：所有 chan_core 操作（构造、push_bar、persist_state、restore_state）
都通过 `ChanWorker.run(fn, ...)` 提交到一个固定的后台线程。FastAPI 路由
里随便哪个线程都可以提交，结果同步返回。
"""
from __future__ import annotations

import queue
import threading
from concurrent.futures import Future
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class ChanWorker:
    def __init__(self, name: str = "chan-worker") -> None:
        self._queue: queue.Queue[Any] = queue.Queue()
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            fn, args, kwargs, future = item
            try:
                result = fn(*args, **kwargs)
                future.set_result(result)
            except BaseException as e:  # noqa: BLE001
                future.set_exception(e)

    def run(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """同步提交并等待结果。fn 在 worker 线程里执行。"""
        future: Future[T] = Future()
        self._queue.put((fn, args, kwargs, future))
        return future.result()

    def shutdown(self, wait: bool = True) -> None:
        self._queue.put(None)
        if wait:
            self._thread.join(timeout=5)
