#!/usr/bin/env python3
"""BTC chan backend — FastAPI server for charting_library frontend"""
import sys, os

print('>>> Starting BTC chan server...')

for mod, pkg in [('fastapi', 'fastapi'), ('uvicorn', 'uvicorn'), ('ccxt', 'ccxt')]:
    try: __import__(mod)
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

FREQ_MAP = {
    "D": (KL_TYPE.K_DAY, "1d"), "1d": (KL_TYPE.K_DAY, "1d"),
    "240": (KL_TYPE.K_60M, "4h"), "120": (KL_TYPE.K_60M, "2h"),
    "60": (KL_TYPE.K_60M, "1h"), "60m": (KL_TYPE.K_60M, "1h"), "1h": (KL_TYPE.K_60M, "1h"),
    "30": (KL_TYPE.K_30M, "30m"), "30m": (KL_TYPE.K_30M, "30m"),
    "15": (KL_TYPE.K_15M, "15m"), "15m": (KL_TYPE.K_15M, "15m"),
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

@app.get("/api/health")
def health(): return {"status": "ok"}

@app.get("/api/bars")
@app.get("/bars")
def bars(symbol: str = "BTCUSDT", freq: str = "D", exchange: str = "BINANCE",
        from_: str = Query(None, alias="from"), to_: str = Query(None, alias="to"), limit: int = 500):
    import ccxt
    ex = ccxt.binance({'options': {'defaultType': 'spot'}})
    _, tf = FREQ_MAP.get(freq, (None, "1d"))
    o = ex.fetch_ohlcv("BTC/USDT", tf, limit=limit)
    return {"s": "ok", "bars": [{"t": i[0] // 1000, "o": i[1], "h": i[2], "l": i[3], "c": i[4], "v": i[5]} for i in o]}

@app.get("/api/chan")
@app.get("/chan")
def chan(symbol: str = "BTCUSDT", freq: str = "D", exchange: str = "BINANCE",
        from_: str = Query(None, alias="from"), to_: str = Query(None, alias="to")):
    c = _get_chan("BTC/USDT", freq)[0]

    # Build Bi list (frontend expects: idx, dir, t0, p0, t1, p1, sure, seg_idx)
    bis = []
    for bi in c.bi_list:
        b = {
            "idx": bi.idx,
            "dir": "UP" if bi.is_up else "DOWN",
            "t0": bi.begin_klc.idx,
            "p0": round(bi.get_begin_val(), 4),
            "t1": bi.end_klc.idx,
            "p1": round(bi.get_end_val(), 4),
            "sure": bi.is_sure if hasattr(bi, "is_sure") else True,
            "seg_idx": bi.seg_idx if hasattr(bi, "seg_idx") else None,
        }
        bis.append(b)

    # Build Seg list
    segs = []
    for i, seg in enumerate(c.seg_list):
        s = {
            "id": i,
            "dir": "UP" if seg.is_up else "DOWN",
            "t0": seg.start_bi.begin_klc.idx if hasattr(seg, "start_bi") and seg.start_bi else 0,
            "p0": round(seg.get_begin_val(), 4),
            "t1": seg.end_bi.end_klc.idx if hasattr(seg, "end_bi") and seg.end_bi else 0,
            "p1": round(seg.get_end_val(), 4),
            "sure": seg.is_sure if hasattr(seg, "is_sure") else True,
            "zs_count": len(seg.zs_lst) if hasattr(seg, "zs_lst") else 0,
            "element_count": len(seg.bi_list) if hasattr(seg, "bi_list") else 0,
        }
        segs.append(s)

    # Build Zs list
    zs_list = []
    for zs in c.zs_list:
        z = {
            "idx": 0,
            "dir": "UP" if zs.is_up else "DOWN",
            "t0": 0, "t1": 0,
            "low": zs.low, "high": zs.high,
            "peak_low": zs.low, "peak_high": zs.high,
            "is_sure": zs.is_sure if hasattr(zs, "is_sure") else True,
            "dir": "UP",
            "sub_zs_count": 0, "element_count": 0,
        }
        zs_list.append(z)

    # Build BSP list
    bsps = []
    for bi in c.bi_list:
        if hasattr(bi, "bsp") and bi.bsp is not None:
            b = bi.bsp
            ts = 0
            if b.klu and hasattr(b.klu, "time") and hasattr(b.klu.time, "ts"):
                ts = b.klu.time.ts
            import re
            _types = re.findall(r"'([^']+)'", str(b.type)) if b.type else []
            bsps.append({
                "element_idx": bi.idx,
                "klu_idx": b.klu.klc.idx if b.klu and hasattr(b.klu, "klc") else 0,
                "t": ts,
                "is_buy": b.is_buy,
                "is_segbsp": False,
                "is_target": True,
                "types": _types,
                "features": {},
            })

    return {
        "symbol": symbol,
        "freq": freq,
        "bis": bis,
        "segs": segs,
        "zs": zs_list,
        "segzs": [],
        "bsps": bsps,
        "seg_bsps": [],
    }

@app.get("/api/chan/build")
@app.get("/chan/build")
def chan_build(symbol: str = "BTCUSDT", freq: str = "D"):
    _get_chan("BTC/USDT", freq)
    return {"status": "ok"}

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
