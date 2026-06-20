#!/usr/bin/env python3
"""BTC chan backend — FastAPI server for charting_library frontend"""
import sys,os

# ── Check dependencies first ──
missing = []
try: import fastapi
except ImportError: missing.append('fastapi')
try: import uvicorn
except ImportError: missing.append('uvicorn')
try: import ccxt
except ImportError: missing.append('ccxt')

if missing:
    print(f'❌ 缺少依赖: {", ".join(missing)}')
    print(f'   请运行: pip install {" ".join(missing)}')
    sys.exit(1)

from fastapi import FastAPI,Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import json

# ── chan.py path ──
chan_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'chanpy')
if not os.path.isdir(chan_path):
    print(f'❌ chanpy 目录不存在: {chan_path}')
    print(f'   server/app.py 必须在项目根目录下运行: python server/app.py')
    sys.exit(1)
sys.path.insert(0, chan_path)

from Common.CEnum import KL_TYPE,DATA_SRC
from Chan import CChan
from ChanConfig import CChanConfig

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

FREQ_MAP = {
    "D": KL_TYPE.K_DAY, "60m": KL_TYPE.K_60M, "30m": KL_TYPE.K_30M,
    "15m": KL_TYPE.K_15M, "5m": KL_TYPE.K_5M, "1m": KL_TYPE.K_1M,
}
TF_CCXT = {"D": "1d", "60m": "1h", "30m": "30m", "15m": "15m", "5m": "5m", "1m": "1m"}
CACHE = {}

def get_chan(symbol, freq):
    key = (symbol, freq)
    if key in CACHE:
        return CACHE[key]
    kl = FREQ_MAP.get(freq, KL_TYPE.K_DAY)
    chan = CChan(code=symbol, begin_time=None, end_time=None, data_src=DATA_SRC.CCXT,
        lv_list=[kl],
        config=CChanConfig({"trigger_step": False, "divergence_rate": 0.7, "min_zs_cnt": 0, "bs_type": "1,2,3a,1p,2s,3b"}))
    CACHE[key] = chan
    return chan

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/bars")
def bars(symbol: str = "BTCUSDT", freq: str = "D", limit: int = 500):
    ex = ccxt.binance()
    tf = TF_CCXT.get(freq, "1d")
    o = ex.fetch_ohlcv("BTC/USDT", tf, limit=limit)
    return [{"time": i[0] // 1000, "open": i[1], "high": i[2], "low": i[3], "close": i[4], "volume": i[5]} for i in o]

@app.get("/api/chan")
def chan(symbol: str = "BTCUSDT", freq: str = "D"):
    c = get_chan("BTC/USDT", freq)[0]
    bars, pens, segs, zs_list, bsp_list = [], [], [], [], []
    for k in c:
        ts = str(k.time)
        bars.append({"time": k.idx, "open": k.open, "high": k.high, "low": k.low, "close": k.close, "volume": 0, "datetime": ts})
    for bi in c.bi_list:
        pens.append({"start_idx": bi.begin_klc.idx, "end_idx": bi.end_klc.idx,
            "start_price": round(bi.get_begin_val(), 4), "end_price": round(bi.get_end_val(), 4),
            "direction": "up" if bi.is_up else "down"})
    for seg in c.seg_list:
        segs.append({"start_idx": 0, "end_idx": 0, "start_price": round(seg.get_begin_val(), 4),
            "end_price": round(seg.get_end_val(), 4), "direction": "up" if seg.is_up else "down"})
    for zs in c.zs_list:
        if hasattr(zs, "low") and hasattr(zs, "high"):
            zs_list.append({"top": zs.high, "bottom": zs.low, "start_idx": 0, "end_idx": 0})
    for bi in c.bi_list:
        if hasattr(bi, "bsp") and bi.bsp is not None:
            b = bi.bsp
            bsp_list.append({"idx": b.klu.klc.idx, "price": b.klu.close, "is_buy": b.is_buy, "types": [str(b.type)] if b.type else []})
    return {"bars": bars, "pens": pens, "segments": segs, "zhongshu": zs_list, "bsps": bsp_list,
        "build_info": {"bars_count": len(bars), "pens_count": len(pens)}}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3000)
