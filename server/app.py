#!/usr/bin/env python3
"""BTC chan backend — FastAPI server for charting_library frontend"""
import sys, os, traceback

print('>>> Starting BTC chan server...')

# ── Check dependencies ──
for mod, pkg in [('fastapi', 'fastapi'), ('uvicorn', 'uvicorn'), ('ccxt', 'ccxt')]:
    try:
        __import__(mod)
    except ImportError:
        print(f'ERROR: {mod} not installed')
        print(f'  Run: pip install {pkg}')
        sys.exit(1)

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import json

# ── chan.py path ──
chan_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'chanpy')
if not os.path.isdir(chan_path):
    print(f'ERROR: chanpy directory not found: {chan_path}')
    print(f'  Must run from project root: python server/app.py')
    sys.exit(1)
sys.path.insert(0, chan_path)

from Common.CEnum import KL_TYPE, DATA_SRC
from Chan import CChan
from ChanConfig import CChanConfig

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

FREQ_MAP = {"D": KL_TYPE.K_DAY, "60m": KL_TYPE.K_60M, "30m": KL_TYPE.K_30M, "15m": KL_TYPE.K_15M}
TF_CCXT = {"D": "1d", "60m": "1h", "30m": "30m", "15m": "15m"}
CACHE = {}

def get_chan(symbol, freq):
    key = (symbol, freq)
    if key in CACHE: return CACHE[key]
    kl = FREQ_MAP.get(freq, KL_TYPE.K_DAY)
    chan = CChan(code=symbol, begin_time=None, end_time=None, data_src=DATA_SRC.CCXT, lv_list=[kl],
        config=CChanConfig({"trigger_step": False, "divergence_rate": 0.7, "min_zs_cnt": 0, "bs_type": "1,2,3a,1p,2s,3b"}))
    CACHE[key] = chan
    return chan

@app.get("/api/health")
def health(): return {"status": "ok"}

@app.get("/api/bars")
def bars(symbol: str = "BTCUSDT", freq: str = "D", limit: int = 500):
    ex = ccxt.binance()
    tf = TF_CCXT.get(freq, "1d")
    o = ex.fetch_ohlcv("BTC/USDT", tf, limit=limit)
    return [{"time": i[0] // 1000, "open": i[1], "high": i[2], "low": i[3], "close": i[4], "volume": i[5]} for i in o]

@app.get("/api/chan")
def chan(symbol: str = "BTCUSDT", freq: str = "D"):
    c = get_chan("BTC/USDT", freq)[0]
    bars = [{"time": k.idx, "open": k.open, "high": k.high, "low": k.low, "close": k.close, "volume": 0} for k in c]
    pens = [{"start_idx": bi.begin_klc.idx, "end_idx": bi.end_klc.idx,
        "start_price": round(bi.get_begin_val(), 4), "end_price": round(bi.get_end_val(), 4),
        "direction": "up" if bi.is_up else "down"} for bi in c.bi_list]
    segs = [{"start_idx": 0, "end_idx": 0, "start_price": round(seg.get_begin_val(), 4),
        "end_price": round(seg.get_end_val(), 4), "direction": "up" if seg.is_up else "down"} for seg in c.seg_list]
    zs_list = [{"top": zs.high, "bottom": zs.low} for zs in c.zs_list if hasattr(zs, "low") and hasattr(zs, "high")]
    bsp_list = [{"idx": b.klu.klc.idx, "price": b.klu.close, "is_buy": b.is_buy, "types": [str(b.type)]} 
        for bi in c.bi_list if hasattr(bi, "bsp") and bi.bsp is not None for b in [bi.bsp]]
    return {"bars": bars, "pens": pens, "segments": segs, "zhongshu": zs_list, "bsps": bsp_list,
        "build_info": {"bars_count": len(bars), "pens_count": len(pens)}}

if __name__ == "__main__":
    print('>>> Running on http://0.0.0.0:3000')
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
