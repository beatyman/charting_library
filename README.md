# BTC 缠论多级别区间套 TradingView 看盘系统

4 级别 2×2 布局 + 缠论指标（笔/段/中枢/BSP）+ Python 后端实时计算。

## 快速启动

```bash
# 1. 安装后端依赖
pip install fastapi uvicorn ccxt

# 2. 启动 Python 后端 (:3000)
python3 server/app.py

# 3. 安装前端依赖并启动
npm install
npm run dev
# → http://localhost:5173
```

## 架构

```
浏览器 2×2 Grid                  Python 后端 :3000
┌────日线────┬────1H────┐         server/app.py
│  TradingView + 缠论指标 │  ←──→  chanpy/ (chan.py)
├────30m─────┬────15m───┤         CCXT → Binance BTC
└────────────┴──────────┘
```

## API 端点

| 端点 | 说明 |
|---|---|
| `GET /api/health` | 健康检查 |
| `GET /api/bars?s=...&freq=...` | K 线数据 |
| `GET /api/chan?s=...&freq=...` | 缠论结构（笔/段/中枢/BSP） |

## 零外部依赖

- 前端：Vite + React + TradingView (charting_library 内嵌)
- 后端：仅需 `pip install fastapi uvicorn ccxt`
- 缠论引擎：chanpy/ (Vespa314/chan.py，内嵌)
- 数据源：Binance 公开 API（无需 API Key）

## 开发

```bash
npm run build    # 生产构建 → dist/
npm run preview  # 预览生产构建
```
