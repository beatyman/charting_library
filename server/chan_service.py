"""缠论结构服务（纯内存版，历史数据模式）。

数据流：
- 首次访问 (exch, sym, freq)：从 CSV 加载 → push_bar 全量 → 构造 FullSnapshot
- 之后 /chan 查询：内存 LRU 切片返回，瞬时
- 服务重启：内存全失效，下次访问触发 5-17s 首算

不落盘、不接收新 K。如果将来要恢复实时推送，看 git 历史 v0.5。
"""
from __future__ import annotations

import bisect
import threading
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import chan_core
import numpy as np
import pandas as pd

from quant.backtest import localAPI
from quant.common.config_loader import load_chan_config_yaml
from quant.server.chan_worker import ChanWorker


# ---------- 工具 ---------- #

def _to_unix_sec(ts) -> int:
    if hasattr(ts, "timestamp"):
        return int(ts.timestamp())
    return int(ts)


def _bi_endpoints(direction: str, high: float, low: float, t0: int, t1: int):
    if direction == "UP":
        return (t0, low), (t1, high)
    return (t0, high), (t1, low)


def _parse_ts(s: str | None) -> int | None:
    if not s:
        return None
    ts = pd.Timestamp(s)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Shanghai")
    return int(ts.timestamp())


# ---------- 数据结构 ---------- #

@dataclass
class FullSnapshot:
    exchange: str
    symbol: str
    freq: str
    csv_mtime: float = 0.0
    bar_count: int = 0
    version: int = 1
    bis: list[dict] = field(default_factory=list)
    segs: list[dict] = field(default_factory=list)
    zs: list[dict] = field(default_factory=list)
    segzs: list[dict] = field(default_factory=list)
    bsps: list[dict] = field(default_factory=list)
    seg_bsps: list[dict] = field(default_factory=list)


@dataclass
class ChanSnapshot:
    """按 [from, to] 切片后返回结构。不含 bars。"""
    symbol: str
    freq: str
    start: int | None
    end: int | None
    version: int = 0
    bis: list[dict] = field(default_factory=list)
    segs: list[dict] = field(default_factory=list)
    zs: list[dict] = field(default_factory=list)
    segzs: list[dict] = field(default_factory=list)
    bsps: list[dict] = field(default_factory=list)
    seg_bsps: list[dict] = field(default_factory=list)


@dataclass
class LiveState:
    """常驻内存的活动状态：MultiChan 实例 + bars 历史 + 当前 snapshot。

    bars 历史用于 _extract_snapshot 里的 KLU idx → time 映射。
    """
    exchange: str
    symbol: str
    freq: str
    multi: Any                                   # chan_core.MultiChan（只在 worker 线程访问）
    bar_times: list[int] = field(default_factory=list)
    bar_highs: list[float] = field(default_factory=list)
    bar_lows: list[float] = field(default_factory=list)
    snapshot: FullSnapshot | None = None
    version: int = 1
    last_pushed_t: int = 0


# ---------- 主服务 ---------- #

class ChanService:
    def __init__(
        self,
        worker: ChanWorker,
        data_root: Path | str = localAPI.DATA_ROOT_DEFAULT,
        cache_dir: Path | str | None = None,  # 保留参数兼容，但已不使用
        memory_cache_size: int = 8,
    ):
        _ = cache_dir  # 纯内存模式，不落盘
        self.worker = worker
        self.data_root = Path(data_root)
        self._live_lock = threading.Lock()
        self._mem = lru_cache(maxsize=memory_cache_size)(self._load_inner)
        # (exchange, symbol, freq) → LiveState（仅供 snapshot + bars 时间映射）
        self._live: dict[tuple[str, str, str], LiveState] = {}

    # --------- 对外接口 --------- #
    def get(self, symbol, freq, start=None, end=None, exchange="SHFE") -> ChanSnapshot:
        full = self._get_full(exchange.upper(), symbol.upper(), freq)
        return self._slice(full, _parse_ts(start), _parse_ts(end))

    def build(self, exchange, symbol, freq, force=False) -> dict:
        exch, sym = exchange.upper(), symbol.upper()
        if force:
            self._invalidate(exch, sym, freq)
        full = self._get_full(exch, sym, freq)
        return {
            "exchange": exch, "symbol": sym, "freq": freq,
            "bars": full.bar_count, "version": full.version,
            "bis": len(full.bis), "segs": len(full.segs),
            "zs": len(full.zs), "segzs": len(full.segzs), "bsps": len(full.bsps),
            "csv_mtime": full.csv_mtime,
        }

    # --------- 内部：加载/缓存 --------- #
    def _get_full(self, exchange, symbol, freq) -> FullSnapshot:
        # 不加全局锁；lru_cache 自身在 GIL 下读取是安全的。
        # 极端并发可能让首次 build 跑两次（lru_cache 只在最终缓存一次），可接受。
        return self._mem(exchange, symbol, freq)

    def _invalidate(self, exchange, symbol, freq) -> None:
        self._mem.cache_clear()
        with self._live_lock:
            self._live.pop((exchange, symbol, freq), None)

    def _load_inner(self, exchange, symbol, freq) -> FullSnapshot:
        # 纯内存模式：不读盘、不写盘，每次都重算。
        # LRU 缓存（self._mem）保证一个 server 进程内同一 (sym, freq) 只算一次。
        return self._compute_and_persist(exchange, symbol, freq)

    def _csv_mtime(self, exchange, symbol, freq) -> float:
        raw_period = "day" if freq in ("1d", "day") else "1min"
        path = self.data_root / exchange / symbol / f"{symbol}_{raw_period}.csv"
        return path.stat().st_mtime if path.exists() else 0.0

    def _compute_and_persist(self, exchange, symbol, freq) -> FullSnapshot:
        t0 = time.perf_counter()
        bars_local = localAPI.load_bars(exchange, symbol, freq, None, None, self.data_root)
        print(f"[chan_service] {exchange}/{symbol}/{freq} 加载 {len(bars_local)} 根 "
              f"({time.perf_counter()-t0:.1f}s)")

        snap = self.worker.run(self._build_live, exchange, symbol, freq, bars_local)
        snap.csv_mtime = self._csv_mtime(exchange, symbol, freq)

        print(
            f"[chan_service] full done {exchange}/{symbol}/{freq}: "
            f"bars={snap.bar_count} bis={len(snap.bis)} segs={len(snap.segs)} "
            f"zs={len(snap.zs)} bsps={len(snap.bsps)}  total {time.perf_counter()-t0:.1f}s"
        )
        return snap

    # --------- 在 worker 线程内：构建 / 推进 --------- #
    def _build_live(self, exchange, symbol, freq, bars_local) -> FullSnapshot:
        """全量构建 LiveState（MultiChan + bars 历史）+ 抽 snapshot。纯内存。"""
        t0 = time.perf_counter()
        multi = chan_core.MultiChan(freqs=[freq], base_freq=freq, config_yaml=load_chan_config_yaml())
        for b in bars_local:
            multi.push_bar(b.dt, b.open, b.high, b.low, b.close, b.volume)
        print(f"[chan_service]   push_bar × {len(bars_local)}: {time.perf_counter()-t0:.1f}s")

        bar_times = [int(b.dt.timestamp()) for b in bars_local]
        bar_highs = [b.high for b in bars_local]
        bar_lows = [b.low for b in bars_local]

        live = LiveState(
            exchange=exchange, symbol=symbol, freq=freq,
            multi=multi,
            bar_times=bar_times, bar_highs=bar_highs, bar_lows=bar_lows,
            version=1,
            last_pushed_t=bar_times[-1] if bar_times else 0,
        )
        live.snapshot = self._extract_snapshot(live, exchange, symbol, freq)
        with self._live_lock:
            self._live[(exchange, symbol, freq)] = live
        return live.snapshot

    # --------- snapshot 抽取（worker 内） --------- #
    @staticmethod
    def _extract_snapshot(live: LiveState, exchange: str, symbol: str, freq: str) -> FullSnapshot:
        bar_times = live.bar_times
        bar_high_np = np.asarray(live.bar_highs, dtype="float64")
        bar_low_np = np.asarray(live.bar_lows, dtype="float64")

        v = live.multi.view(freq)
        klcs = v.klcs_to_arrow()
        bis_tbl = v.bis_to_arrow()
        segs_tbl = v.segs_to_arrow()
        zs_tbl = v.zs_to_arrow()
        segzs_tbl = v.segzs_to_arrow()
        bsps_tbl = v.bsps_to_arrow()
        seg_bsps_tbl = v.seg_bsps_to_arrow()

        klc_idx_np = klcs.column("idx").to_numpy()
        klc_klu_start_np = klcs.column("klu_start_idx").to_numpy()
        klc_klu_end_np = klcs.column("klu_end_idx").to_numpy()
        klc_range = dict(zip(
            klc_idx_np.tolist(),
            zip(klc_klu_start_np.tolist(), klc_klu_end_np.tolist()),
        ))

        snap = FullSnapshot(
            exchange=exchange, symbol=symbol, freq=freq,
            bar_count=len(bar_times), version=live.version,
        )
        snap.bis = ChanService._bis_payload(bis_tbl, klc_range, bar_times,
                                            bar_high_np, bar_low_np)
        snap.segs = ChanService._segs_payload(segs_tbl, snap.bis)
        snap.zs = ChanService._zs_payload(zs_tbl, snap.bis)
        snap.segzs = ChanService._zs_payload(segzs_tbl, snap.segs)
        snap.bsps = ChanService._bsps_payload(bsps_tbl, bar_times)
        snap.seg_bsps = ChanService._bsps_payload(seg_bsps_tbl, bar_times)
        return snap

    # --------- 切片 --------- #
    @staticmethod
    def _slice(full: FullSnapshot, start_unix, end_unix) -> ChanSnapshot:
        out = ChanSnapshot(
            symbol=full.symbol, freq=full.freq,
            start=start_unix, end=end_unix, version=full.version,
        )
        if start_unix is None and end_unix is None:
            out.bis = full.bis
            out.segs = full.segs
            out.zs = full.zs
            out.segzs = full.segzs
            out.bsps = full.bsps
            out.seg_bsps = full.seg_bsps
            return out

        lo = start_unix if start_unix is not None else -(1 << 62)
        hi = end_unix if end_unix is not None else (1 << 62)

        for src, dst_name in ((full.bsps, "bsps"), (full.seg_bsps, "seg_bsps")):
            ts = [x["t"] for x in src]
            a = bisect.bisect_left(ts, lo)
            b = bisect.bisect_right(ts, hi)
            setattr(out, dst_name, src[a:b])

        out.bis = [x for x in full.bis if x["t1"] >= lo and x["t0"] <= hi]
        out.segs = [x for x in full.segs if x["t1"] >= lo and x["t0"] <= hi]
        out.zs = [x for x in full.zs if x["t1"] >= lo and x["t0"] <= hi]
        out.segzs = [x for x in full.segzs if x["t1"] >= lo and x["t0"] <= hi]
        return out

    # --------- payload 转换 --------- #
    @staticmethod
    def _bis_payload(bis_tbl, klc_range, bar_times, bar_high_np, bar_low_np) -> list[dict]:
        if bis_tbl.num_rows == 0:
            return []
        cols = bis_tbl.to_pydict()
        out: list[dict] = []
        for i in range(bis_tbl.num_rows):
            sk = cols["start_klc_idx"][i]
            ek = cols["end_klc_idx"][i]
            direction = cols["direction"][i]
            bi_high = cols["high"][i]
            bi_low = cols["low"][i]
            sk_range = klc_range.get(sk)
            ek_range = klc_range.get(ek)
            if not sk_range or not ek_range:
                continue
            if direction == "UP":
                t0 = ChanService._argmin_low_time(sk_range, bar_times, bar_low_np)
                t1 = ChanService._argmax_high_time(ek_range, bar_times, bar_high_np)
                p0, p1 = bi_low, bi_high
            else:
                t0 = ChanService._argmax_high_time(sk_range, bar_times, bar_high_np)
                t1 = ChanService._argmin_low_time(ek_range, bar_times, bar_low_np)
                p0, p1 = bi_high, bi_low
            out.append({
                "idx": cols["idx"][i], "dir": direction,
                "t0": t0, "p0": p0, "t1": t1, "p1": p1,
                "sure": bool(cols["is_sure"][i]),
                "seg_idx": cols["seg_idx"][i],
            })
        return out

    @staticmethod
    def _argmin_low_time(klu_range, bar_times, bar_low_np) -> int:
        lo, hi = klu_range
        rel = int(bar_low_np[lo:hi + 1].argmin())
        return bar_times[lo + rel]

    @staticmethod
    def _argmax_high_time(klu_range, bar_times, bar_high_np) -> int:
        lo, hi = klu_range
        rel = int(bar_high_np[lo:hi + 1].argmax())
        return bar_times[lo + rel]

    @staticmethod
    def _segs_payload(segs_tbl, bis) -> list[dict]:
        if segs_tbl.num_rows == 0:
            return []
        bi_by_idx = {b["idx"]: b for b in bis}
        cols = segs_tbl.to_pydict()
        out = []
        for i in range(segs_tbl.num_rows):
            sb = bi_by_idx.get(cols["start_element_id"][i])
            eb = bi_by_idx.get(cols["end_element_id"][i])
            if not sb or not eb:
                continue
            direction = cols["direction"][i]
            (t0, p0), (t1, p1) = _bi_endpoints(direction, cols["high"][i], cols["low"][i],
                                               sb["t0"], eb["t1"])
            out.append({
                "id": cols["id"][i], "dir": direction,
                "t0": t0, "p0": p0, "t1": t1, "p1": p1,
                "sure": bool(cols["is_sure"][i]),
                "zs_count": cols["zs_count"][i],
                "element_count": cols["element_count"][i],
            })
        return out

    @staticmethod
    def _zs_payload(zs_tbl, elements) -> list[dict]:
        if zs_tbl.num_rows == 0:
            return []
        elem_by_key = {}
        for e in elements:
            k = e.get("idx", e.get("id"))
            elem_by_key[k] = e
        cols = zs_tbl.to_pydict()
        out = []
        for i in range(zs_tbl.num_rows):
            be = elem_by_key.get(cols["begin_element_id"][i])
            ee = elem_by_key.get(cols["end_element_id"][i])
            if not be or not ee:
                continue
            out.append({
                "idx": cols["idx"][i], "dir": cols["direction"][i],
                "t0": be["t0"], "t1": ee["t1"],
                "low": cols["low"][i], "high": cols["high"][i],
                "peak_low": cols["peak_low"][i], "peak_high": cols["peak_high"][i],
                "is_sure": bool(cols["is_sure"][i]),
                "sub_zs_count": cols["sub_zs_count"][i],
                "element_count": cols["element_count"][i],
            })
        return out

    @staticmethod
    def _bsps_payload(bsps_tbl, bar_times) -> list[dict]:
        if bsps_tbl.num_rows == 0:
            return []
        cols = bsps_tbl.to_pydict()
        feature_cols = [
            "divergence_rate", "bsp_bi_amp", "zs_cnt", "bsp1_bi_amp",
            "bsp2_retrace_rate", "bsp2_break_bi_amp", "bsp2_bi_amp",
            "bsp2s_retrace_rate", "bsp2s_break_bi_amp", "bsp2s_bi_amp",
            "bsp2s_lv", "bsp3_zs_height", "bsp3_bi_amp",
        ]
        n_bars = len(bar_times)
        out = []
        for i in range(bsps_tbl.num_rows):
            klu_idx = cols["klu_idx"][i]
            t = bar_times[klu_idx] if 0 <= klu_idx < n_bars else 0
            features: dict[str, Any] = {}
            for fc in feature_cols:
                v = cols[fc][i]
                if v is not None and v == v:
                    features[fc] = v
            out.append({
                "element_idx": cols["element_idx"][i],
                "klu_idx": klu_idx,
                "t": t,
                "is_buy": bool(cols["is_buy"][i]),
                "is_segbsp": bool(cols["is_segbsp"][i]),
                "is_target": bool(cols["is_target"][i]),
                "types": list(cols["types"][i] or []),
                "features": features,
            })
        return out
