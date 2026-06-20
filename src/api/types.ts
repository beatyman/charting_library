export interface Bar {
  t: number // unix seconds
  o: number
  h: number
  l: number
  c: number
  v: number
}

export type Direction = 'UP' | 'DOWN'

export interface Bi {
  idx: number
  dir: Direction
  t0: number
  p0: number
  t1: number
  p1: number
  sure: boolean
  seg_idx: number | null
}

export interface Seg {
  id: number
  dir: Direction
  t0: number
  p0: number
  t1: number
  p1: number
  sure: boolean
  zs_count: number
  element_count: number
}

export interface Zs {
  idx: number
  dir: Direction
  t0: number
  t1: number
  low: number
  high: number
  peak_low: number
  peak_high: number
  is_sure: boolean
  sub_zs_count: number
  element_count: number
}

export interface Bsp {
  element_idx: number
  klu_idx: number
  t: number
  is_buy: boolean
  is_segbsp: boolean
  is_target: boolean
  types: string[]
  features: Record<string, number>
}

export interface ChanSlice {
  symbol: string
  freq: string
  bis: Bi[]
  segs: Seg[]
  zs: Zs[]
  segzs: Zs[]
  bsps: Bsp[]
  seg_bsps: Bsp[]
}

export interface ContractSpec {
  symbol: string
  exchange: string
  multiplier: number
  price_tick: number
  margin_rate: number
  fee_rate: number
  fee_fixed: number
  intraday_fee_mult: number
}

// ---------- 回测 ---------- //

export interface BacktestStrategyParam {
  name: string
  type: 'int' | 'float' | 'str' | 'bool'
  default: any
  label?: string
  help?: string
}
export interface BacktestStrategy {
  name: string
  label: string
  params: BacktestStrategyParam[]
}

export interface BacktestStartResp {
  session_id: string
  exchange: string
  symbol: string
  freq: string
  strategy: string
  backfilled: number
  remaining: number
  init_cash: number
}

export interface BacktestFill {
  t: number
  side: 'BUY' | 'SELL' | null
  action: 'OPEN' | 'CLOSE'
  qty: number
  price: number
  fee: number
  realized_pnl: number
  reason: string
}

export interface BacktestEquityPoint {
  t: number
  equity: number
  cash: number
  floating_pnl: number
  position_qty: number
}

export interface BacktestStepResp {
  session_id: string
  pushed: number
  remaining: number
  bars: Bar[]
  new_fills: BacktestFill[]
  new_equity: BacktestEquityPoint[]
}

export interface BacktestState {
  session_id: string
  cursor: number
  remaining: number
  fills_count: number
  n_trades: number
  initial_cash: number
  equity: number
  total_pnl: number
  total_return_pct: number
  max_drawdown_pct: number
  win_rate_pct: number
  sharpe: number
  position_qty: number
}

/** /replay/start 响应。 */
export interface ReplayStartResp {
  session_id: string
  exchange: string
  symbol: string
  freq: string
  remaining: number
  counts: Record<string, number>
}

/** /replay/{id}/step 响应。bars 是这次步进推入的 K，按时间升序。 */
export interface ReplayStepResp {
  session_id: string
  pushed: number
  remaining: number
  bars: Bar[]
  counts: Record<string, number>
  delta: Record<string, number>
  // new: 末尾增量结构。schema 跟 /chan 不同（来自 arrow.to_pylist 原始字段），
  // 前端不直接用它画图，而是 step 后再调一次 /chan 拿统一格式。
  new: Record<string, any[]>
}

export interface ChanBuildInfo {
  exchange: string
  symbol: string
  freq: string
  bars: number
  bis: number
  segs: number
  zs: number
  segzs: number
  bsps: number
  csv_mtime: number
}
