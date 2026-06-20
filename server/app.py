#!/usr/bin/env python3
"""BTC chan backend — FastAPI server (raw HTTP, no ccxt)"""
import sys, os, json, time, requests

print('>>> Starting BTC chan server...')

for mod in ['fastapi', 'uvicorn', 'requests']:
    try: __import__(mod)
    except ImportError:
        print(f'ERROR: {mod} not installed. Run: pip install {mod}')
        sys.exit(1)

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

chan_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'chanpy')
if not os.path.isdir(chan_path):
    print(f'ERROR: chanpy not found: {chan_path}')
    sys.exit(1)
sys.path.insert(0, chan_path)

from Common.CEnum import KL_TYPE, DATA_SRC, DATA_FIELD
from Common.CTime import CTime
from KLine.KLine_Unit import CKLine_Unit
from Chan import CChan
from ChanConfig import CChanConfig

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

TF_MAP = {"D": "1d", "1d": "1d", "60": "1h", "60m": "1h", "30": "30m", "30m": "30m", "15": "15m", "15m": "15m"}
KL_MAP = {"D": KL_TYPE.K_DAY, "1d": KL_TYPE.K_DAY, "60": KL_TYPE.K_60M, "60m": KL_TYPE.K_60M, "30": KL_TYPE.K_30M, "30m": KL_TYPE.K_30M, "15": KL_TYPE.K_15M, "15m": KL_TYPE.K_15M}
CACHE = {}

def fetch_binance(symbol, interval, limit):
    """Raw Binance API — no ccxt market loading"""
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        return [{"t": int(d[0] // 1000), "o": float(d[1]), "h": float(d[2]), "l": float(d[3]), "c": float(d[4]), "v": float(d[5])} for d in data]
    except Exception as e:
        print(f"Binance API error: {e}")
        return []

@app.get("/api/health")
@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/api/bars")
@app.get("/bars")
def bars(symbol: str = "BTCUSDT", freq: str = "D", limit: int = 500):
    tf = TF_MAP.get(freq, "1d")
    result = fetch_binance("BTCUSDT", tf, limit)
    return {"s": "ok", "bars": result}

def _get_chan(symbol, freq):
    key = (symbol, freq)
    if key in CACHE: return CACHE[key]
    kl = KL_MAP.get(freq, KL_TYPE.K_DAY)
    tf = TF_MAP.get(freq, "1d")
    bars = fetch_binance("BTCUSDT", tf, 500)
    klines = []
    for b in bars:
        t = b["t"]
        dt = time.gmtime(t)
        has_minute = tf != "1d"
        if has_minute: ct = CTime(dt.tm_year, dt.tm_mon, dt.tm_mday, dt.tm_hour, dt.tm_min, auto=False)
        else: ct = CTime(dt.tm_year, dt.tm_mon, dt.tm_mday, 0, 0, auto=True)
        klines.append(CKLine_Unit({DATA_FIELD.FIELD_TIME: ct, DATA_FIELD.FIELD_OPEN: b["o"], DATA_FIELD.FIELD_HIGH: b["h"], DATA_FIELD.FIELD_LOW: b["l"], DATA_FIELD.FIELD_CLOSE: b["c"]}, autofix=True))
    chan = CChan(code=symbol, begin_time=None, end_time=None, data_src=1, lv_list=[kl],
        config=CChanConfig({"trigger_step": True, "divergence_rate": 0.7, "min_zs_cnt": 0, "bs_type": "1,2,3a,1p,2s,3b"}))
    for klu in klines: chan.trigger_load({kl: [klu]})
    CACHE[key] = chan
    return chan

@app.get("/api/chan")
@app.get("/chan")
def chan(symbol: str = "BTCUSDT", freq: str = "D"):
    chan_obj = _get_chan("BTC/USDT", freq)
    c = chan_obj[0]
    
    # Build idx -> time mapping from original bars
    bar_times = {}
    for i, bar in enumerate(fetch_binance("BTCUSDT", TF_MAP.get(freq, "1d"), 500)):
        bar_times[i] = bar["t"]
    
    import re
    bis = []
    for bi in c.bi_list:
        bis.append({"idx": bi.idx, "dir": "UP" if bi.is_up else "DOWN",
            "t0": bar_times.get(bi.begin_klc.idx, 0), "p0": round(bi.get_begin_val(), 4),
            "t1": bar_times.get(bi.end_klc.idx, 0), "p1": round(bi.get_end_val(), 4),
            "sure": True, "seg_idx": bi.seg_idx if hasattr(bi, "seg_idx") else None})
    segs = []
    for i, seg in enumerate(c.seg_list):
        segs.append({"id": i, "dir": "UP" if seg.is_up else "DOWN",
            "t0": bar_times.get(seg.start_bi.begin_klc.idx, 0) if hasattr(seg, "start_bi") and seg.start_bi else 0,
            "p0": round(seg.get_begin_val(), 4),
            "t1": bar_times.get(seg.end_bi.end_klc.idx, 0) if hasattr(seg, "end_bi") and seg.end_bi else 0,
            "p1": round(seg.get_end_val(), 4),
            "sure": True, "zs_count": len(seg.zs_lst) if hasattr(seg, "zs_lst") else 0,
            "element_count": len(seg.bi_list) if hasattr(seg, "bi_list") else 0})
    zs_list = []
    for zs in c.zs_list:
        zs_list.append({"idx": 0, "dir": "UP", "t0": 0, "t1": 0,
            "low": zs.low, "high": zs.high, "peak_low": zs.low, "peak_high": zs.high,
            "is_sure": True, "sub_zs_count": 0, "element_count": 0})
    bsps = []
    for bi in c.bi_list:
        if hasattr(bi, "bsp") and bi.bsp is not None:
            b = bi.bsp; ts = 0
            if b.klu and hasattr(b.klu, "time") and hasattr(b.klu.time, "ts"): ts = b.klu.time.ts
            _types = re.findall(r"'([^']+)'", str(b.type)) if b.type else []
            bsps.append({"element_idx": bi.idx, "klu_idx": b.klu.klc.idx if b.klu and hasattr(b.klu, "klc") else 0,
                "t": ts, "is_buy": b.is_buy, "is_segbsp": False, "is_target": True, "types": _types, "features": {}})
    return {"symbol": symbol, "freq": freq, "bis": bis, "segs": segs, "zs": zs_list, "segzs": [], "bsps": bsps, "seg_bsps": []}

@app.get("/api/chan/build")
def chan_build(symbol: str = "BTCUSDT", freq: str = "D"):
    _get_chan("BTC/USDT", freq)
    return {"status": "ok"}

if __name__ == "__main__":
    print('>>> Running on http://0.0.0.0:3000')
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
