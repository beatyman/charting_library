"""FastAPI 路由：纯历史数据模式。

- /health
- /bars        K 线（1d 直读 day.csv，其它周期由 1min resample）
- /chan/build  触发 (exch, sym, freq) 全量缠论计算（首次进入对应 freq 时调一次）
- /chan        按 [from, to] 切片返回笔/段/中枢/BSP
- /replay/*    回放会话
"""
from __future__ import annotations

import threading
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from quant.backtest import localAPI
from quant.server.backtest_service import BacktestService
from quant.server.chan_service import ChanService
from quant.server.chan_worker import ChanWorker
from quant.server.replay_service import ReplayService


# 1min → 目标周期的 pandas resample 规则（小写 h/d 是 pandas 新版要求）
_FREQ_RULE = {
    "1m": "1min", "3m": "3min", "5m": "5min", "10m": "10min",
    "15m": "15min", "30m": "30min",
    "1h": "1h", "2h": "2h", "4h": "4h",
    "1d": "1d", "day": "1d",
}


def create_app(
    data_root: Path | str = localAPI.DATA_ROOT_DEFAULT,
    cache_size: int = 16,
) -> FastAPI:
    cache_lock = threading.Lock()
    data_root = Path(data_root)

    worker = ChanWorker()
    chan_service = ChanService(worker=worker, data_root=data_root)
    replay_service = ReplayService(worker=worker, chan_service=chan_service,
                                   data_root=data_root)
    backtest_service = BacktestService(worker=worker, chan_service=chan_service,
                                       data_root=data_root)

    def _load_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"CSV 不存在: {path}")
        df = pd.read_csv(
            path,
            parse_dates=["datetime"],
            usecols=["datetime", "open", "high", "low", "close", "volume"],
            dtype={
                "open": "float64", "high": "float64",
                "low": "float64", "close": "float64",
                "volume": "float64",
            },
        )
        df["datetime"] = (
            df["datetime"].dt.tz_localize("Asia/Shanghai").dt.tz_convert("UTC")
        )
        return df.set_index("datetime").sort_index()

    @lru_cache(maxsize=cache_size)
    def _raw_1min(exchange: str, symbol: str) -> pd.DataFrame:
        return _load_csv(data_root / exchange / symbol / f"{symbol}_1min.csv")

    @lru_cache(maxsize=cache_size)
    def _raw_day(exchange: str, symbol: str) -> pd.DataFrame:
        # 日线直读 day.csv，避免 1min resample 到 1d 把交易日切在 UTC 00:00
        # 而原日线 CSV 是 Beijing 00:00（差 8 小时），导致 chan/bars 时间戳错位
        return _load_csv(data_root / exchange / symbol / f"{symbol}_day.csv")

    def get_raw(exchange: str, symbol: str) -> pd.DataFrame:
        with cache_lock:
            return _raw_1min(exchange.upper(), symbol.upper())

    def get_raw_day(exchange: str, symbol: str) -> pd.DataFrame:
        with cache_lock:
            return _raw_day(exchange.upper(), symbol.upper())

    def parse_ts(s: str | None) -> pd.Timestamp | None:
        if not s:
            return None
        ts = pd.Timestamp(s)
        if ts.tzinfo is None:
            ts = ts.tz_localize("Asia/Shanghai")
        return ts.tz_convert("UTC")

    def resample_to(df_1min: pd.DataFrame, freq: str) -> pd.DataFrame:
        rule = _FREQ_RULE.get(freq)
        if rule is None:
            raise HTTPException(400, f"不支持的周期 {freq}")
        if rule == "1min":
            return df_1min
        g = df_1min.resample(rule, label="left", closed="left")
        return pd.DataFrame({
            "open": g["open"].first(),
            "high": g["high"].max(),
            "low": g["low"].min(),
            "close": g["close"].last(),
            "volume": g["volume"].sum(),
        }).dropna(subset=["open"])

    app = FastAPI(title="chan-core HTTP", version="0.6.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["*"],
    )

    @app.on_event("shutdown")
    def _on_shutdown() -> None:
        worker.shutdown()

    # ---------- /health ---------- #
    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "data_root": str(data_root)}

    # ---------- /symbols ---------- #
    @lru_cache(maxsize=1)
    def _scan_symbols() -> list[dict]:
        """扫 data_root：每个 <exchange>/<symbol>/<symbol>_1min.csv (或 _day.csv) 算一个 ticker。"""
        out: list[dict] = []
        if not data_root.exists():
            return out
        for exch_dir in sorted(data_root.iterdir()):
            if not exch_dir.is_dir():
                continue
            for sym_dir in sorted(exch_dir.iterdir()):
                if not sym_dir.is_dir():
                    continue
                has_1min = any(sym_dir.glob(f"{sym_dir.name}_1min.csv"))
                has_day = any(sym_dir.glob(f"{sym_dir.name}_day.csv"))
                if not (has_1min or has_day):
                    continue
                out.append({
                    "exchange": exch_dir.name,
                    "symbol": sym_dir.name,
                    "ticker": f"{exch_dir.name}:{sym_dir.name}",
                    "has_1min": has_1min,
                    "has_day": has_day,
                })
        return out

    @app.get("/symbols")
    def list_symbols(
        q: str | None = Query(None, description="模糊搜索（按 symbol 或 exchange 包含匹配）"),
        exchange: str | None = Query(None, description="精确过滤交易所"),
    ) -> dict:
        items = _scan_symbols()
        if exchange:
            ex = exchange.upper()
            items = [x for x in items if x["exchange"] == ex]
        if q:
            qu = q.upper()
            items = [x for x in items if qu in x["symbol"] or qu in x["exchange"]]
        return {"n": len(items), "symbols": items}

    # ---------- /bars ---------- #
    @app.get("/bars")
    def get_bars(
        symbol: str = Query(..., description="品种代号，如 RB"),
        freq: str = Query("5m"),
        exchange: str = Query("SHFE"),
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
        limit: int | None = Query(None, ge=1, description="返回末尾 limit 根（TradingView countBack）"),
    ) -> dict:
        is_day = freq in ("1d", "day")
        try:
            raw = get_raw_day(exchange, symbol) if is_day else get_raw(exchange, symbol)
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))

        mpb = {"1m": 1, "3m": 3, "5m": 5, "10m": 10, "15m": 15, "30m": 30,
               "1h": 60, "2h": 120, "4h": 240, "1d": 1440}

        start_ts = parse_ts(start)
        end_ts = parse_ts(end)

        if limit is not None and start_ts is None:
            sliced = raw.loc[:end_ts] if end_ts is not None else raw
            rows_needed = limit if is_day else max(mpb.get(freq, 5) * limit * 2, 256)
            df = sliced.iloc[-rows_needed:]
        else:
            df = raw.loc[start_ts:end_ts] if (start_ts is not None or end_ts is not None) else raw

        out_df = df if is_day else resample_to(df, freq)
        if limit is not None and len(out_df) > limit:
            out_df = out_df.tail(limit)

        bars = [
            {
                "t": int(ts.timestamp()),
                "o": float(row.open), "h": float(row.high),
                "l": float(row.low), "c": float(row.close),
                "v": float(row.volume),
            }
            for ts, row in out_df.iterrows()
        ]
        return {"symbol": symbol, "freq": freq, "n": len(bars), "bars": bars}

    # ---------- /chan ---------- #
    @app.post("/chan/build")
    def chan_build(
        symbol: str = Query(...),
        freq: str = Query("1m"),
        exchange: str = Query("SHFE"),
        force: bool = Query(False, description="强制重算"),
    ) -> dict:
        try:
            return chan_service.build(exchange, symbol, freq, force=force)
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))

    @app.get("/chan")
    def chan_get(
        symbol: str = Query(...),
        freq: str = Query("1m"),
        exchange: str = Query("SHFE"),
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
    ) -> dict:
        """只返回结构（笔/段/中枢/BSP）；bars 走 /bars 端点。"""
        try:
            snap = chan_service.get(symbol, freq, start, end, exchange=exchange)
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))
        return {
            "symbol": snap.symbol, "freq": snap.freq,
            "bis": snap.bis, "segs": snap.segs,
            "zs": snap.zs, "segzs": snap.segzs,
            "bsps": snap.bsps,
            "seg_bsps": snap.seg_bsps,
        }

    # ---------- /replay ---------- #
    @app.post("/replay/start")
    def replay_start(
        symbol: str = Query(...),
        freq: str = Query("1m"),
        exchange: str = Query("SHFE"),
        at: str = Query(..., description="回放起点时刻 (ISO / YYYY-MM-DD HH:MM:SS)"),
    ) -> dict:
        try:
            return replay_service.start(exchange, symbol, freq, at)
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))

    @app.post("/replay/{session_id}/step")
    def replay_step(
        session_id: str,
        n: int = Query(1, ge=1, le=10000, description="单次推进的 bar 数"),
    ) -> dict:
        try:
            return replay_service.step(session_id, n=n)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @app.get("/replay/{session_id}/chan")
    def replay_chan(
        session_id: str,
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
    ) -> dict:
        """从回放 session 自己的 MultiChan 取切片：chan 状态仅基于已 push 进 session
        的 bar 计算，跟着 step 进度演化，最新一段如未确认会保持 sure=false。"""
        try:
            return replay_service.chan(session_id, start, end)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @app.delete("/replay/{session_id}")
    def replay_stop(session_id: str) -> dict:
        replay_service.stop(session_id)
        return {"ok": True}

    @app.get("/replay")
    def replay_list() -> dict:
        return {"sessions": replay_service.list_sessions()}

    # ---------- /contract ---------- #
    @app.get("/contract/{symbol}")
    def get_contract(symbol: str) -> dict:
        """返回合约规格，前端 backtest 面板预填用。"""
        from quant.backtest import contracts as _c
        spec = _c.get_spec(symbol)
        return {
            "symbol": spec.symbol, "exchange": spec.exchange,
            "multiplier": spec.multiplier, "price_tick": spec.price_tick,
            "margin_rate": spec.margin_rate,
            "fee_rate": spec.fee_rate, "fee_fixed": spec.fee_fixed,
            "intraday_fee_mult": spec.intraday_fee_mult,
        }

    # ---------- /backtest ---------- #
    @app.get("/backtest/strategies")
    def backtest_strategies() -> dict:
        return {"strategies": backtest_service.strategies()}

    @app.post("/backtest/start")
    def backtest_start(payload: dict) -> dict:
        """完整 JSON body 提交，方便携带策略参数 dict。
        body 字段：
          exchange, symbol, freq, at, strategy, params, init_cash, slippage_ticks
          margin_rate?, fee_rate?, fee_fixed?  - 覆盖合约 spec 默认值，None 走默认
        """
        def _opt_float(k: str) -> float | None:
            v = payload.get(k)
            if v is None or v == "":
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        try:
            return backtest_service.start(
                exchange=payload.get("exchange", "SHFE"),
                symbol=payload["symbol"],
                freq=payload.get("freq", "5m"),
                at=payload["at"],
                strategy_name=payload["strategy"],
                strategy_params=payload.get("params") or {},
                init_cash=float(payload.get("init_cash", 1_000_000)),
                slippage_ticks=float(payload.get("slippage_ticks", 1.0)),
                margin_rate=_opt_float("margin_rate"),
                fee_rate=_opt_float("fee_rate"),
                fee_fixed=_opt_float("fee_fixed"),
            )
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))
        except KeyError as e:
            raise HTTPException(400, str(e))
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.post("/backtest/{session_id}/step")
    def backtest_step(
        session_id: str,
        n: int = Query(1, ge=1, le=10000),
    ) -> dict:
        try:
            return backtest_service.step(session_id, n=n)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @app.get("/backtest/{session_id}/state")
    def backtest_state(session_id: str) -> dict:
        try:
            return backtest_service.state(session_id)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @app.get("/backtest/{session_id}/chan")
    def backtest_chan(
        session_id: str,
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
    ) -> dict:
        try:
            return backtest_service.chan(session_id, start, end)
        except KeyError as e:
            raise HTTPException(404, str(e))

    @app.delete("/backtest/{session_id}")
    def backtest_stop(session_id: str) -> dict:
        backtest_service.stop(session_id)
        return {"ok": True}

    @app.get("/backtest")
    def backtest_list() -> dict:
        return {"sessions": backtest_service.list_sessions()}

    return app


def _reload_app() -> FastAPI:
    """uvicorn --reload 入口工厂。"""
    from quant import envs
    return create_app(data_root=str(envs.chan_data_root()))
