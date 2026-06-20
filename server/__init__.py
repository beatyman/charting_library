"""HTTP 数据服务：本地 CSV → JSON bars，给前端 TradingView 用。

启动：
    python -m quant.server [--host 127.0.0.1] [--port 8000]

端点：
    GET /health
    GET /bars  ?symbol=RB&freq=5m&exchange=SHFE&from=...&to=...&limit=...

时间戳：t 字段是 Unix 秒，符合 TradingView UDF 期望。
"""
