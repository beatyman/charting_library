import { TradingViewChart } from './chart/TradingViewChart'
import './App.css'

export default function App() {
  return (
    <div className="app">
      <TradingViewChart
        exchange="SHFE"
        symbol="RB"
        freq="5m"
      />
    </div>
  )
}
