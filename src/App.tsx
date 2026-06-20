import { TradingViewChart } from './chart/TradingViewChart'
import './App.css'

const CHARTS = [
  { exchange: 'BINANCE', symbol: 'BTCUSDT', freq: 'D'   as const },
  { exchange: 'BINANCE', symbol: 'BTCUSDT', freq: '60m' as const },
  { exchange: 'BINANCE', symbol: 'BTCUSDT', freq: '30m' as const },
  { exchange: 'BINANCE', symbol: 'BTCUSDT', freq: '15m' as const },
]

export default function App() {
  return (
    <div className="multi-chart-grid">
      {CHARTS.map(c => (
        <div className="chart-cell" key={c.freq}>
          <TradingViewChart
            exchange={c.exchange}
            symbol={c.symbol}
            freq={c.freq}
          />
        </div>
      ))}
    </div>
  )
}
