#!/usr/bin/env python3
"""BTC chan backend — FastAPI server for charting_library frontend"""
import sys, os

print('>>> Starting BTC chan server...')

for mod, pkg in [('fastapi', 'fastapi'), ('uvicorn', 'uvicorn'), ('ccxt', 'ccxt')]:
    try:
        __import__(mod)
    except ImportError:
        print(f'ERROR: {mod} not installed. Run: pip install {pkg}')
        sys.exit(1)

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List

chan_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'chanpy')
if not os.path.isdir(chan_path):
    print(f'ERROR: chanpy not found: {chan_path}. Must run from project root.')
    sys.exit(1)
sys.path.insert(0, chan_path)

from Common.CEnum import KL_TYPE, DATA_SRC
from Chan import CChan
from ChanConfig import CChanConfig

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# freq mapping: TV resolution → chan KL_TYPE + ccxt timeframe
FREQ_MAP = {
    "D": (KL_TYPE.K_DAY, "1d"), "1d": (KL_TYPE.K_DAY, "1d"),
    "240": (KL_TYPE.K_60M, "4h"), "120": (KL_TYPE.K_60M, "2h"),
    "60": (KL_TYPE.K_60M, "1h"), "60m": (KL_TYPE.K_60M, "1h"), "1h": (KL_TYPE.K_60M, "1h"),
    "30": (KL_TYPE.K_30M, "30m"), "30m": (KL_TYPE.K_30M, "30m"),
    "15": (KL_TYPE.K_15M, "15m"), "15m": (KL_TYPE.K_15M, "15m"),
    "5": (KL_TYPE.K_5M, "5m"), "5m": (KL_TYPE.K_5M, "5m"),
    "1": (KL_TYPE.K_1M, "1m"), "1m": (KL_TYPE.K_1M, "1m"),
}

CACHE = {}

def _get_chan(symbol, freq):
    key = (symbol, freq)
    if key in CACHE: return CACHE[key]
    kl, _ = FREQ_MAP.get(freq, (KL_TYPE.K_DAY, "1d"))
    chan = CChan(code=symbol, begin_time=None, end_time=None, data_src=DATA_SRC.CCXT, lv_list=[kl],
        config=CChanConfig({"trigger_step": False, "divergence_rate": 0.7, "min_zs_cnt": 0, "bs_type": "1,2,3a,1p,2s,3b"}))
    CACHE[key] = chan
    return chan

# ── API ──

@app.get("/api/health")
def health(): return {"status": "ok"}

@app.get("/api/bars")
@app.get("/bars")
def bars(symbol: str = "BTCUSDT", freq: str = "D", exchange: str = "BINANCE",
        from_: str = Query(None, alias="from"), to_: str = Query(None, alias="to"),
        limit: int = 500):
    import ccxt
    ex = ccxt.binance({'options': {'defaultType': 'spot'}})
    _, tf = FREQ_MAP.get(freq, (None, "1d"))
    o = ex.fetch_ohlcv("BTC/USDT", tf, limit=limit)
    result = [{"t": i[0] // 1000, "o": i[1], "h": i[2], "l": i[3], "c": i[4], "v": i[5]} for i in o]
    return {"s": "ok", "bars": result}

@app.get("/api/chan")
@app.get("/chan")
def chan(symbol: str = "BTCUSDT", freq: str = "D", exchange: str = "BINANCE"):
    c = _get_chan("BTC/USDT", freq)[0]
    bars = []
    for k in c:
        for klu in k.lst:
            bars.append({"t": klu.time.ts if hasattr(klu.time,'ts') else str(klu.time),
                "o": klu.open, "h": klu.high, "l": klu.low, "c": klu.close, "v": klu.volume if hasattr(klu,'volume') else 0})
    pens, segs, zs_list, bsp_list = [], [], [], []
    for bi in c.bi_list:
        pens.append({"start_idx": bi.begin_klc.idx, "end_idx": bi.end_klc.idx,
            "start_price": round(bi.get_begin_val(), 4), "end_price": round(bi.get_end_val(), 4),
            "direction": "up" if bi.is_up else "down"})
    for seg in c.seg_list:
        segs.append({"start_idx": 0, "end_idx": 0, "start_price": round(seg.get_begin_val(), 4),
            "end_price": round(seg.get_end_val(), 4), "direction": "up" if seg.is_up else "down"})
    for zs in c.zs_list:
        if hasattr(zs, "low") and hasattr(zs, "high"):
            zs_list.append({"top": zs.high, "bottom": zs.low})
    for bi in c.bi_list:
        if hasattr(bi, "bsp") and bi.bsp is not None:
            b = bi.bsp
            bsp_list.append({"idx": b.klu.klc.idx, "price": b.klu.close, "is_buy": b.is_buy, "types": [str(b.type)] if b.type else []})
    return {"bars": bars, "pens": pens, "segments": segs, "zhongshu": zs_list, "bsps": bsp_list,
        "build_info": {"bars_count": len(bars), "pens_count": len(pens)}}

# /chan/build — stub for frontend compatibility
@app.get("/api/chan/build")
@app.get("/chan/build")
def chan_build(symbol: str = "BTCUSDT", freq: str = "D"):
    _get_chan("BTC/USDT", freq)
    return {"status": "ok"}

# /api/config — contract spec for frontend
@app.get("/api/config")
def config(exchange: str = "BINANCE"):
    return {"contracts": [
        {"symbol": "BTCUSDT", "name": "BTC/USDT", "price_scale": 2, "min_price_move": 0.01,
         "session": "24x7", "exchange": "BINANCE", "supported_freqs": ["1m","5m","15m","30m","1h","1d"]}
    ]}

if __name__ == "__main__":
    print('>>> Running on http://0.0.0.0:3000')
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
