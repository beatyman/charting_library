"""回测会话：跟 ReplayService 类似的「逐 K 推进」模式，但额外有 Broker + Strategy。

跟 BacktestEngine.run() 区别：BacktestEngine 一口气把所有 bar 跑完；这里把 live
阶段拆成 step(n)，前端能逐根驱动。回测开始时把 t <= at_ts 的 bar 作为 backfill
一次性灌入引擎（不触发策略），然后等 step。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import chan_core
import pandas as pd

from quant.backtest import contracts, localAPI, metrics
from quant.common.config_loader import load_chan_config_yaml
from quant.backtest.runner import BacktestRunner
from quant.server.chan_service import (
    ChanService, LiveState, _parse_ts,
)
from quant.server.chan_worker import ChanWorker
from quant.strategies import Strategy, build_strategy, list_strategies


# bars_per_year 走 metrics.estimate_bars_per_year 自动推导，旧表移到 run.py 的 fallback


@dataclass
class _BtSession:
    session_id: str
    exchange: str
    symbol: str
    freq: str
    multi: Any
    runner: BacktestRunner
    strategy: Strategy
    bars_all: list                          # 全部 bars (localAPI.Bar)
    cursor: int = 0                         # 下一根要 push 的 index
    backfill_cnt: int = 0
    # 已 push 的 bar 历史，给 chan_service._extract_snapshot 做 klu_idx → time 映射
    bar_times: list[int] = field(default_factory=list)
    bar_highs: list[float] = field(default_factory=list)
    bar_lows: list[float] = field(default_factory=list)
    # 自上次 step 起的差量基准
    last_fill_count: int = 0
    last_equity_count: int = 0
    prev_bsp_count: int = 0                 # diff_bsps 使用
    last_active: float = field(default_factory=time.time)


class BacktestService:
    """回测会话管理。所有 chan_core 调用走 ChanWorker。"""

    def __init__(
        self,
        worker: ChanWorker,
        chan_service: ChanService,
        data_root: Path | str = localAPI.DATA_ROOT_DEFAULT,
        session_ttl_sec: int = 3600,
    ):
        self.worker = worker
        self.chan_service = chan_service
        self.data_root = Path(data_root)
        self.ttl = session_ttl_sec
        self._sessions: dict[str, _BtSession] = {}
        self._lock = threading.Lock()

    # --------- API --------- #
    def strategies(self) -> list[dict]:
        return list_strategies()

    def start(
        self, exchange: str, symbol: str, freq: str, at: str,
        strategy_name: str,
        strategy_params: dict | None = None,
        init_cash: float = 1_000_000.0,
        slippage_ticks: float = 1.0,
        margin_rate: float | None = None,
        fee_rate: float | None = None,
        fee_fixed: float | None = None,
    ) -> dict:
        exch, sym = exchange.upper(), symbol.upper()
        at_ts = _parse_ts(at)
        if at_ts is None:
            raise ValueError("at 必须是有效时间")

        all_bars = localAPI.load_bars(exch, sym, freq, None, None, self.data_root)

        def _create() -> _BtSession:
            multi = chan_core.MultiChan(freqs=[freq], base_freq=freq, config_yaml=load_chan_config_yaml())
            base_spec = contracts.get_spec(sym)
            # 应用用户覆盖：dataclasses.replace 生成新实例（ContractSpec frozen=True）
            from dataclasses import replace
            spec = replace(
                base_spec,
                margin_rate=base_spec.margin_rate if margin_rate is None else margin_rate,
                fee_rate=base_spec.fee_rate if fee_rate is None else fee_rate,
                fee_fixed=base_spec.fee_fixed if fee_fixed is None else fee_fixed,
            )
            strategy = build_strategy(strategy_name, strategy_params or {})
            runner = BacktestRunner(
                multi=multi, spec=spec, strategy=strategy,
                base_freq=freq, initial_cash=init_cash,
                slippage_ticks=slippage_ticks,
            )

            # backfill：t <= at_ts 的 bar 全部灌进引擎，不触发策略
            bar_times: list[int] = []
            bar_highs: list[float] = []
            bar_lows: list[float] = []
            cursor = 0
            for b in all_bars:
                bt = int(b.dt.timestamp())
                if bt <= at_ts:
                    multi.push_bar(b.dt, b.open, b.high, b.low, b.close, b.volume)
                    bar_times.append(bt)
                    bar_highs.append(b.high)
                    bar_lows.append(b.low)
                    cursor += 1
                else:
                    break  # bars 按 dt 升序

            view = multi.view(freq)
            session = _BtSession(
                session_id=uuid.uuid4().hex,
                exchange=exch, symbol=sym, freq=freq,
                multi=multi, runner=runner, strategy=strategy,
                bars_all=all_bars, cursor=cursor,
                backfill_cnt=cursor,
                bar_times=bar_times, bar_highs=bar_highs, bar_lows=bar_lows,
                prev_bsp_count=view.bsp_count(),
            )
            # runner 的 _prev_bsp_count 也要同步到 backfill 后的水平
            runner._prev_bsp_count = view.bsp_count()
            strategy.on_start()
            return session

        session = self.worker.run(_create)
        with self._lock:
            self._sessions[session.session_id] = session
        return {
            "session_id": session.session_id,
            "exchange": exch, "symbol": sym, "freq": freq,
            "strategy": strategy_name,
            "backfilled": session.backfill_cnt,
            "remaining": len(all_bars) - session.cursor,
            "init_cash": init_cash,
        }

    def step(self, session_id: str, n: int = 1) -> dict:
        session = self._get(session_id)

        def _step() -> dict:
            pushed_bars: list[dict] = []
            broker = session.runner.broker
            for _ in range(n):
                if session.cursor >= len(session.bars_all):
                    break
                bar = session.bars_all[session.cursor]

                # 让 runner 跑完整 live 流程（feed open quote → flush pending → push_bar →
                # 触发策略 → feed close quote → mark-to-market）
                session.runner.on_bar(bar)

                bt = int(bar.dt.timestamp())
                session.bar_times.append(bt)
                session.bar_highs.append(bar.high)
                session.bar_lows.append(bar.low)
                pushed_bars.append({
                    "t": bt, "o": bar.open, "h": bar.high,
                    "l": bar.low, "c": bar.close, "v": bar.volume,
                })
                session.cursor += 1

            # 差量返回新增的 fills 和 equity 点
            new_fills: list[dict] = []
            for f in broker.fills[session.last_fill_count:]:
                new_fills.append({
                    "t": int(f.dt.timestamp()),
                    "side": f.side.name if f.side else None,
                    "action": f.action.value,
                    "qty": f.qty, "price": f.price,
                    "fee": f.fee, "realized_pnl": f.realized_pnl,
                    "reason": f.reason,
                })
            session.last_fill_count = len(broker.fills)

            new_equity: list[dict] = []
            for s in broker.equity_curve[session.last_equity_count:]:
                new_equity.append({
                    "t": int(s.dt.timestamp()),
                    "equity": s.equity, "cash": s.cash,
                    "floating_pnl": s.floating_pnl,
                    "position_qty": s.position_qty,
                })
            session.last_equity_count = len(broker.equity_curve)

            session.last_active = time.time()
            return {
                "session_id": session.session_id,
                "pushed": len(pushed_bars),
                "remaining": len(session.bars_all) - session.cursor,
                "bars": pushed_bars,
                "new_fills": new_fills,
                "new_equity": new_equity,
            }

        return self.worker.run(_step)

    def state(self, session_id: str) -> dict:
        """当前累积绩效指标。"""
        session = self._get(session_id)

        def _do() -> dict:
            broker = session.runner.broker
            if not broker.equity_curve:
                return {
                    "session_id": session.session_id,
                    "cursor": session.cursor,
                    "remaining": len(session.bars_all) - session.cursor,
                    "fills_count": 0, "n_trades": 0,
                    "initial_cash": broker.initial_cash,
                    "equity": broker.initial_cash, "total_pnl": 0.0,
                    "total_return_pct": 0.0, "max_drawdown_pct": 0.0,
                    "win_rate_pct": 0.0, "sharpe": 0.0,
                    "position_qty": 0,
                }
            eq_df = pd.DataFrame([{
                "dt": s.dt, "cash": s.cash, "margin": s.margin,
                "equity": s.equity, "floating_pnl": s.floating_pnl,
                "position_qty": s.position_qty,
            } for s in broker.equity_curve])
            m = metrics.compute(
                eq_df, broker.fills,
                initial_cash=broker.initial_cash,
                bars_per_year=None,  # 自动从 equity 数据推导
            )
            return {
                "session_id": session.session_id,
                "cursor": session.cursor,
                "remaining": len(session.bars_all) - session.cursor,
                "fills_count": len(broker.fills),
                "n_trades": m.n_trades,
                "initial_cash": m.initial,
                "equity": m.final,
                "total_pnl": m.final - m.initial,
                "total_return_pct": m.total_return_pct,
                "max_drawdown_pct": m.max_drawdown_pct,
                "win_rate_pct": m.win_rate_pct,
                "sharpe": m.sharpe,
                "position_qty": broker.equity_curve[-1].position_qty,
            }

        return self.worker.run(_do)

    def chan(self, session_id: str, start: str | None, end: str | None) -> dict:
        """从回测 session 自己的 MultiChan 抽切片：chan 仅基于已 push 的 bar 计算，
        跟 step 进度演化，跟 ReplayService.chan 完全一样的处理方式。"""
        session = self._get(session_id)

        def _do() -> dict:
            fake_live = LiveState(
                exchange=session.exchange, symbol=session.symbol, freq=session.freq,
                multi=session.multi,
                bar_times=session.bar_times,
                bar_highs=session.bar_highs, bar_lows=session.bar_lows,
                version=1,
                last_pushed_t=session.bar_times[-1] if session.bar_times else 0,
            )
            full = ChanService._extract_snapshot(
                fake_live, session.exchange, session.symbol, session.freq,
            )
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
                    "cursor": s.cursor,
                    "remaining": len(s.bars_all) - s.cursor,
                    "fills_count": len(s.runner.broker.fills),
                    "last_active": s.last_active,
                }
                for s in self._sessions.values()
            ]

    def gc(self) -> int:
        now = time.time()
        with self._lock:
            expired = [sid for sid, s in self._sessions.items()
                       if now - s.last_active > self.ttl]
            for sid in expired:
                del self._sessions[sid]
        return len(expired)

    # --------- 内部 --------- #
    def _get(self, session_id: str) -> _BtSession:
        with self._lock:
            s = self._sessions.get(session_id)
        if s is None:
            raise KeyError(f"backtest session {session_id} 不存在或已过期")
        return s
