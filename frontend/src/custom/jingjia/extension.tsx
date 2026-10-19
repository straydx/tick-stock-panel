import { memo, useCallback, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Gavel, RefreshCw, Search } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import { boardTag } from '@/components/stock-table/primitives'
import { fmtPrice, fmtPct, priceColorClass } from '@/lib/format'
import type { FrontendExtension } from '@/extensions/types'

/**
 * 竞价选股 — 五维评分竞价选股 + T+1 真实收益记录 (二开扩展)。
 *
 * 后端: backend/app/custom/jingjia.py (L2 扩展, 独立路由 /api/custom/jingjia/select)。
 * 本文件为完整前端扩展(组件 + 路由 + 导航), 零核心文件改动, 删除即可整体卸载。
 *
 * 评分五维: A 竞价量能 25% · B 开盘位置 20% · C 连板位置 15% · D 板块热度 20% · E 竞价形态 20%。
 * 硬过滤: 一字板 (竞价撮合价已顶到涨停价, 无法买入) 直接剔除。
 * T+1 收益: 选股日竞价开盘价(买) → 次日收盘价(卖)。
 */

interface JingjiaDim {
  auction_volume?: number
  open_position?: number
  ladder_position?: number
  sector_heat?: number
  auction_shape?: number
}

interface JingjiaItem {
  symbol: string
  name?: string | null
  consec: number
  auction_price?: number | null
  open?: number | null
  change_pct?: number | null
  matched_vol: number
  unmatched_vol: number
  unmatched_dir: number
  score: number
  dims: JingjiaDim
  concepts: string[]
  t0_pct?: number | null
  t1_pct?: number | null
}

interface JingjiaPayload {
  state: 'ok' | 'no_data'
  as_of: string | null
  items: JingjiaItem[]
  total: number
  one_word_filtered: number
}

async function fetchJingjia(asOf?: string): Promise<JingjiaPayload> {
  const url = `/api/custom/jingjia/select${asOf ? `?as_of=${encodeURIComponent(asOf)}` : ''}`
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`竞价选股请求失败: ${res.status}`)
  }
  return res.json() as Promise<JingjiaPayload>
}

async function fetchDates(): Promise<string[]> {
  const res = await fetch('/api/custom/jingjia/dates')
  if (!res.ok) return []
  const j = (await res.json()) as { dates?: string[] }
  return j.dates ?? []
}

function JingjiaPage() {
  const [query, setQuery] = useState('')
  const [asOf, setAsOf] = useState<string>('')
  const [previewSymbol, setPreviewSymbol] = useState<string | null>(null)
  const [previewName, setPreviewName] = useState<string>('')

  const openStock = useCallback((symbol: string, name?: string | null) => {
    setPreviewSymbol(symbol)
    setPreviewName(name ?? '')
  }, [])

  const q = useQuery({
    queryKey: ['jingjia', 'select', asOf],
    queryFn: () => fetchJingjia(asOf || undefined),
    staleTime: 5 * 60_000,
    retry: 1,
  })

  const datesQ = useQuery({
    queryKey: ['jingjia', 'dates'],
    queryFn: fetchDates,
    staleTime: 10 * 60_000,
    retry: 1,
  })

  const data = q.data

  const rows = useMemo(() => {
    const list = data?.items ?? []
    const s = query.trim().toLowerCase()
    if (!s) return list
    return list.filter(r => `${r.symbol} ${r.name ?? ''}`.toLowerCase().includes(s))
  }, [data, query])

  const t1Vals = (data?.items ?? [])
    .map(i => i.t1_pct)
    .filter((v): v is number => v != null)
  const avgT1 = t1Vals.length ? t1Vals.reduce((a, b) => a + b, 0) / t1Vals.length : null

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="shrink-0">
        <PageHeader
          title="竞价选股"
          subtitle="五维评分竞价选股 · 过滤一字板 · T+1 真实收益记录"
          right={
            <button
              type="button"
              onClick={() => q.refetch()}
              className="inline-flex h-7 items-center gap-1 rounded border border-border bg-base px-2 text-[11px] text-secondary transition-colors hover:text-foreground"
            >
              <RefreshCw className={`h-3 w-3 ${q.isFetching ? 'animate-spin' : ''}`} />
              刷新
            </button>
          }
        />
      </div>

      {/* 统计条 */}
      <div className="flex shrink-0 flex-wrap items-center gap-2 px-5 pt-1">
        <span className="inline-flex items-center gap-1.5 rounded-full border border-border bg-elevated px-2.5 py-1 text-[11px] text-secondary">
          选股日
          <select
            value={asOf || (data?.as_of ?? '')}
            onChange={e => setAsOf(e.target.value)}
            disabled={!data?.as_of && !datesQ.data?.length}
            title="选择历史选股日回看"
            className="cursor-pointer bg-transparent font-mono font-semibold text-foreground outline-none [&>option]:bg-background [&>option]:text-foreground"
          >
            <option value="">{data?.as_of ?? '—'}</option>
            {(datesQ.data ?? []).map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </span>
        <span className="rounded-full border border-border bg-elevated px-2.5 py-1 text-[11px] text-secondary">
          入选 <span className="font-mono font-semibold text-foreground">{data?.total ?? 0}</span> 只
        </span>
        <span className="rounded-full border border-danger/30 bg-danger/8 px-2.5 py-1 text-[11px] text-danger">
          一字板剔除 <span className="font-mono font-semibold">{data?.one_word_filtered ?? 0}</span>
        </span>
        {avgT1 != null && (
          <span
            className="rounded-full border border-border bg-elevated px-2.5 py-1 text-[11px] text-secondary"
            title="入选标的 T+1 收益均值 (竞价开盘价买入 → 次日收盘价卖出)"
          >
            T+1 均值 <span className={`font-mono font-semibold ${priceColorClass(avgT1)}`}>{fmtPct(avgT1)}</span>
          </span>
        )}
        <div className="relative ml-auto">
          <Search className="absolute left-2 top-1.5 h-3.5 w-3.5 text-muted" />
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="搜索代码/名称"
            className="h-7 w-40 rounded border border-border bg-base pl-7 pr-2 text-[11px] text-foreground"
          />
        </div>
      </div>

      {/* 主表 */}
      <div className="min-h-0 flex-1 overflow-auto px-5 pb-4 pt-3">
        <div className="overflow-hidden rounded-card border border-border bg-surface">
          <table className="w-full min-w-[720px] text-xs">
            <thead className="sticky top-0 z-10 bg-surface">
              <tr className="border-b border-border text-[10px] uppercase tracking-wider text-muted">
                <th className="w-10 px-2 py-2 text-right">#</th>
                <th className="px-2 py-2 text-left">代码 / 名称</th>
                <th className="px-2 py-2 text-right" title="选股日开盘价(9:30 成交价)">开盘价</th>
                <th className="px-2 py-2 text-right" title="五维加权评分 (0~100)">评分</th>
                <th className="px-2 py-2 text-left" title="竞价未匹配方向(买压/卖压)">形态</th>
                <th className="px-2 py-2 text-right" title="昨日连板数">连板</th>
                <th className="px-2 py-2 text-right" title="开盘价买入 → 当日收盘价卖出">T+0 收益</th>
                <th className="px-2 py-2 text-right" title="开盘价买入 → 次日收盘价卖出">T+1 收益</th>
              </tr>
            </thead>
            <tbody>
              {q.isLoading ? (
                <tr><td colSpan={8} className="px-3 py-10 text-center text-muted">正在计算竞价选股…</td></tr>
              ) : q.isError ? (
                <tr>
                  <td colSpan={8} className="px-3 py-10 text-center">
                    <div className="flex flex-col items-center gap-2">
                      <span className="text-xs text-secondary">竞价选股计算失败，请检查数据源与网络</span>
                      <button
                        type="button"
                        onClick={() => q.refetch()}
                        disabled={q.isFetching}
                        className="inline-flex items-center gap-1.5 rounded-md border border-border px-2.5 py-1 text-xs text-accent transition-colors hover:bg-elevated disabled:opacity-50"
                      >
                        <RefreshCw className="h-3 w-3" />
                        重试
                      </button>
                    </div>
                  </td>
                </tr>
              ) : rows.length === 0 ? (
                <tr><td colSpan={8} className="px-3 py-10 text-center text-muted">
                  {data?.state === 'no_data' ? '暂无日K数据' : '当前无入选标的'}
                </td></tr>
              ) : (
                rows.map((r, i) => (
                  <JingjiaRowView
                    key={r.symbol}
                    row={r}
                    rank={i + 1}
                    onOpen={openStock}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {previewSymbol && (
        <StockPreviewDialog
          symbol={previewSymbol}
          name={previewName}
          onClose={() => { setPreviewSymbol(null); setPreviewName('') }}
        />
      )}
    </div>
  )
}

const JingjiaRowView = memo(function JingjiaRowView({ row, rank, onOpen }: {
  row: JingjiaItem
  rank: number
  onOpen: (symbol: string, name?: string | null) => void
}) {
  const board = boardTag(row.symbol)
  return (
    <tr
      className="group cursor-pointer border-b border-border/40 transition-colors last:border-0 hover:bg-elevated/50"
      onClick={() => onOpen(row.symbol, row.name)}
    >
      <td className="px-2 py-1.5 text-right font-mono text-[10px] text-muted/70">{rank}</td>
      <td className="px-2 py-1.5">
        <span className="flex min-w-0 items-center gap-1.5">
          <span className="shrink-0 font-mono text-xs text-foreground">{row.symbol}</span>
          <span className="min-w-0 max-w-40 truncate text-xs text-secondary">{row.name ?? '—'}</span>
          {board && (
            <span className={`shrink-0 rounded border px-1 text-[9px] font-bold leading-tight ${board.color}`}>
              {board.label}
            </span>
          )}
          {row.consec > 1 && (
            <span className="shrink-0 rounded border border-amber-500/30 bg-amber-500/10 px-1 text-[9px] font-bold leading-tight text-amber-500" title={`连续 ${row.consec} 日涨停`}>
              {row.consec}连板
            </span>
          )}
        </span>
      </td>
      <td className="px-2 py-1.5 text-right font-mono text-xs text-foreground">
        {row.open != null ? fmtPrice(row.open) : (row.auction_price != null ? fmtPrice(row.auction_price) : '—')}
      </td>
      <td className="px-2 py-1.5 text-right font-mono text-sm font-semibold tabular-nums text-accent">
        {row.score.toFixed(1)}
      </td>
      <td className="px-2 py-1.5">
        {row.unmatched_vol > 0 && row.matched_vol > 0 ? (
          <span
            className={`rounded border px-1 text-[9px] font-medium leading-tight ${
              row.unmatched_dir > 0
                ? 'border-bull/25 bg-bull/10 text-bull'
                : row.unmatched_dir < 0
                  ? 'border-bear/25 bg-bear/10 text-bear'
                  : 'border-border bg-elevated text-secondary'
            }`}
            title={`未匹配 ${row.unmatched_vol.toLocaleString()} 手 · ${row.unmatched_dir > 0 ? '买压' : row.unmatched_dir < 0 ? '卖压' : '平衡'}`}
          >
            {row.unmatched_dir > 0 ? '买压' : row.unmatched_dir < 0 ? '卖压' : '平衡'}
          </span>
        ) : (
          <span className="text-muted/40">—</span>
        )}
      </td>
      <td className="px-2 py-1.5 text-right font-mono text-xs text-secondary">{row.consec}</td>
      <td className={`px-2 py-1.5 text-right font-mono text-xs font-medium tabular-nums ${row.t0_pct == null ? 'text-muted' : priceColorClass(row.t0_pct)}`}>
        {row.t0_pct == null ? '—' : fmtPct(row.t0_pct)}
      </td>
      <td className={`px-2 py-1.5 text-right font-mono text-xs font-medium tabular-nums ${row.t1_pct == null ? 'text-muted' : priceColorClass(row.t1_pct)}`}>
        {row.t1_pct == null ? '—' : fmtPct(row.t1_pct)}
      </td>
    </tr>
  )
})

// ── 扩展注册清单 — 本文件即完整扩展(组件 + 路由 + 导航), 单文件可整体摘除/导入 ──
const extension: FrontendExtension = {
  id: 'jingjia.selector',
  apiVersion: 1,
  routes: [
    { id: 'jingjia', path: '/jingjia', component: JingjiaPage },
  ],
  navigation: [
    { id: 'jingjia', routeId: 'jingjia', label: '竞价选股', icon: Gavel },
  ],
}

export default extension
