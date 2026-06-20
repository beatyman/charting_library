"""回放会话：从某个历史时刻 restore_state，再用 push_bar 逐根推进。

会话状态全部留在 ChanWorker 线程：
- 创建：worker.run 里 restore_state_at_ts，剩余 bars 切片
- 步进：worker.run 里 push_bar N 根，提取增量结构（与上一次对比 count 取尾部）
- 销毁：客户端调 stop 或 TTL 过期自动清理
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import chan_core

from quant.backtest import localAPI
from quant.common.config_loader import load_chan_config_yaml
from quant.server.chan_service import (
    ChanService, LiveState, _parse_ts, _to_unix_sec,
)
from quant.server.chan_worker import ChanWorker


@dataclass
class _Session:
    session_id: str
    exchange: str
    symbol: str
    freq: str
    multi: Any                          # chan_core.MultiChan（只能在 worker 线程访问）
    remaining: list                     # 还未推送的 Bar（localAPI.Bar 列表）
    cursor: int = 0                     # 已推送到 remaining 的位置
    last_counts: dict[str, int] = field(default_factory=dict)
    last_active: float = field(default_factory=time.time)
    # 已 push 进 multi 的 bar 历史，给 _extract_snapshot 做 klu_idx → time 映射
    bar_times: list[int] = field(default_factory=list)
    bar_highs: list[float] = field(default_factory=list)
    bar_lows: list[float] = field(default_factory=list)


class ReplayService:
    """回放会话管理。所有 chan_core 调用走 ChanWorker。"""

    def __init__(
        self,
        worker: ChanWorker,
        chan_service: ChanService,
        data_root: Path | str = localAPI.DATA_ROOT_DEFAULT,
        session_ttl_sec: int = 1800,
    ):
        self.worker = worker
        self.chan_service = chan_service
        self.data_root = Path(data_root)
        self.ttl = session_ttl_sec
        self._sessions: dict[str, _Session] = {}
        self._lock = threading.Lock()

    # --------- API --------- #
    def start(self, exchange: str, symbol: str, freq: str, at: str) -> dict:
        exch, sym = exchange.upper(), symbol.upper()
        at_ts = _parse_ts(at)
        if at_ts is None:
            raise ValueError("at 必须是有效时间")

        all_bars = localAPI.load_bars(exch, sym, freq, None, None, self.data_root)

        def _create() -> _Session:
            # 纯内存模式：起一个独立 MultiChan，把 t <= at_ts 的 bars 全部 push 进去
            # 作为 replay 起点；剩余 bars 留给 step() 逐根喂
            multi = chan_core.MultiChan(freqs=[freq], base_freq=freq, config_yaml=load_chan_config_yaml())
            bar_times: list[int] = []
            bar_highs: list[float] = []
            bar_lows: list[float] = []
            remaining = []
            for b in all_bars:
                bt = int(b.dt.timestamp())
                if bt <= at_ts:
                    multi.push_bar(b.dt, b.open, b.high, b.low, b.close, b.volume)
                    bar_times.append(bt)
                    bar_highs.append(b.high)
                    bar_lows.append(b.low)
                else:
                    remaining.append(b)
            session = _Session(
                session_id=uuid.uuid4().hex,
                exchange=exch, symbol=sym, freq=freq,
                multi=multi, remaining=remaining,
                bar_times=bar_times, bar_highs=bar_highs, bar_lows=bar_lows,
            )
            session.last_counts = self._snapshot_counts(multi, freq)
            return session

        session = self.worker.run(_create)
        with self._lock:
            self._sessions[session.session_id] = session
        return {
            "session_id": session.session_id,
            "exchange": exch, "symbol": sym, "freq": freq,
            "remaining": len(session.remaining),
            "counts": session.last_counts,
        }

    def step(self, session_id: str, n: int = 1) -> dict:
        session = self._get(session_id)

        def _step() -> dict:
            pushed_bars: list[dict] = []
            for _ in range(n):
                if session.cursor >= len(session.remaining):
                    break
                b = session.remaining[session.cursor]
                session.multi.push_bar(b.dt, b.open, b.high, b.low, b.close, b.volume)
                bt = int(b.dt.timestamp())
                session.bar_times.append(bt)
                session.bar_highs.append(b.high)
                session.bar_lows.append(b.low)
                session.cursor += 1
                pushed_bars.append({
                    "t": bt,
                    "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume,
                })

            cur = self._snapshot_counts(session.multi, session.freq)
            delta = {k: cur[k] - session.last_counts.get(k, 0) for k in cur}
            new_tail = self._extract_tail(session.multi, session.freq, delta)
            session.last_counts = cur
            session.last_active = time.time()
            return {
                "session_id": session.session_id,
                "pushed": len(pushed_bars),
                "remaining": len(session.remaining) - session.cursor,
                "bars": pushed_bars,
                "counts": cur,
                "delta": delta,
                "new": new_tail,
            }

        return self.worker.run(_step)

    def chan(self, session_id: str, start: str | None, end: str | None) -> dict:
        """从回放 session 自己的 MultiChan 抽切片，避免使用「全 csv 算完后切片」
        造成的「上帝视角」—— 后者的最后一段笔/段可能因未来确认变成 sure。
        """
        session = self._get(session_id)

        def _do() -> dict:
            # 用 chan_service._extract_snapshot 复用同套 payload 转换，session 临时包成
            # LiveState 结构以满足函数签名（_extract_snapshot 只读 multi/bar_times/
            # bar_highs/bar_lows，不会改 session）。
            fake_live = LiveState(
                exchange=session.exchange, symbol=session.symbol, freq=session.freq,
                multi=session.multi,
                bar_times=session.bar_times,
                bar_highs=session.bar_highs, bar_lows=session.bar_lows,
                version=1,
                last_pushed_t=session.bar_times[-1] if session.bar_times else 0,
            )
            full = ChanService._extract_snapshot(fake_live, session.exchange,
                                                 session.symbol, session.freq)
            snap = ChanService._slice(full, _parse_ts(start), _parse_ts(end))
            return {
                "symbol": snap.symbol, "freq": snap.freq,
                "bis": snap.bis, "segs": snap.segs,
                "zs": snap.zs, "segzs": snap.segzs,
                "bsps": snap.bsps, "seg_bsps": snap.seg_bsps,
            }

        return self.worker.run(_do)

    def stop(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def list_sessions(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "session_id": s.session_id,
                    "exchange": s.exchange, "symbol": s.symbol, "freq": s.freq,
                    "cursor": s.cursor, "remaining": len(s.remaining) - s.cursor,
                    "last_active": s.last_active,
                }
                for s in self._sessions.values()
            ]

    def gc(self) -> int:
        """清理 TTL 过期的会话，返回清理数量。"""
        now = time.time()
        with self._lock:
            expired = [sid for sid, s in self._sessions.items()
                       if now - s.last_active > self.ttl]
            for sid in expired:
                del self._sessions[sid]
        return len(expired)

    # --------- 内部 --------- #
    def _get(self, session_id: str) -> _Session:
        with self._lock:
            s = self._sessions.get(session_id)
        if s is None:
            raise KeyError(f"session {session_id} 不存在或已过期")
        return s

    @staticmethod
    def _snapshot_counts(multi, freq: str) -> dict[str, int]:
        v = multi.view(freq)
        return {
            "bars": v.bar_count(),
            "bis": v.bi_count(),
            "segs": v.seg_count(),
            "zs": v.zs_count(),
            "segzs": v.segzs_count(),
            "bsps": v.bsp_count(),
        }

    @staticmethod
    def _extract_tail(multi, freq: str, delta: dict[str, int]) -> dict[str, list[dict]]:
        """按 delta 取每类结构的末尾 N 条（N 至少 1，包含可能被更新的最后一根）。"""
        v = multi.view(freq)
        out: dict[str, list[dict]] = {"bis": [], "segs": [], "zs": [], "segzs": [], "bsps": []}

        def _tail(tbl, n: int) -> list[dict]:
            if tbl.num_rows == 0 or n <= 0:
                return []
            return tbl.slice(max(0, tbl.num_rows - n), n).to_pylist()

        # 多取一根，覆盖"最后一笔被延长"等场景
        if delta.get("bis", 0) >= 0:
            out["bis"] = _tail(v.bis_to_arrow(), max(1, delta["bis"] + 1))
        if delta.get("segs", 0) >= 0:
            out["segs"] = _tail(v.segs_to_arrow(), max(1, delta["segs"] + 1))
        if delta.get("zs", 0) >= 0:
            out["zs"] = _tail(v.zs_to_arrow(), max(1, delta["zs"] + 1))
        if delta.get("segzs", 0) >= 0:
            out["segzs"] = _tail(v.segzs_to_arrow(), max(1, delta["segzs"] + 1))
        if delta.get("bsps", 0) >= 0:
            out["bsps"] = _tail(v.bsps_to_arrow(), max(1, delta["bsps"] + 1))
        return out
