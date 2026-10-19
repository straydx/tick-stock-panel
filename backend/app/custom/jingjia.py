"""竞价选股扩展 — 完全解耦的 L2 二开模块。

按 docs/secondary-development.md 的 L2 扩展契约实现:
  - 后端: custom/jingjia.py 通过 setup() + registrar.include_router() 注册独立路由,
    不改动任何核心文件; 删除本文件即可整体卸载。
  - 数据访问全部复用核心仓库/服务, 不平行实现第二套数据逻辑。

策略口径 (五维评分, 参考 jingjia-stock-selector 思路, 数据源全部来自本项目):
  A. 竞价量能   (25%) — eltdx auctions.series 真实竞价已匹配量/未匹配量 + 开盘初量能
  B. 开盘位置   (20%) — 竞价撮合价 vs 昨收的高开幅度
  C. 连板位置   (15%) — consecutive_limit_ups 连板梯队
  D. 板块热度   (20%) — market_mainline 主线排行 + 概念映射 (可选, 缺数据时该项归 0)
  E. 竞价形态   (20%) — 竞价未匹配方向 (买压/卖压) + 高开质量

硬过滤:
  - 一字板剔除: 竞价撮合价已顶到涨停价 (open >= limit_up_price) → 无法买入, 直接剔除。

T+1 真实收益记录:
  - 选股日竞价开盘价 (买) → 次日收盘价 (卖), 收益 = next_close / auction_open - 1。
  - 复用 auction_benchmark._prev_trading_day/_next_trading_day 的交易日口径。

数据契约:
  - enriched 数据经 ScreenerService._load_enriched_for_date 读取, 含 signal_limit_up /
    signal_broken_limit_up / consecutive_limit_ups / limit_up_price / open / close 等。
  - 一字板判定沿用 screener._one_word_limit_expr 口径: open==high==low==close 且涨停;
    竞价阶段用「竞价撮合价 >= 涨停判定价」判定 (更贴近集合竞价实际)。
  - eltdx 竞价接口: client.auctions.series(code, date) → AuctionSeries.points,
    每点含 price(虚拟撮合价)/matched_volume(已匹配量)/unmatched_volume(未匹配量)/
    unmatched_direction_raw(未匹配方向)。
"""
import contextlib
import logging
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import Request

from app.extensions import (
    BACKEND_EXTENSION_API_VERSION,
    BackendExtensionRegistrar,
    ExtensionContext,
)

logger = logging.getLogger(__name__)

EXTENSION_ID = "jingjia.selector"
EXTENSION_API_VERSION = BACKEND_EXTENSION_API_VERSION

# 五维评分权重 (合计 1.0)
_WEIGHTS = {
    "auction_volume": 0.25,   # A 竞价量能
    "open_position": 0.20,    # B 开盘位置
    "ladder_position": 0.15,  # C 连板位置
    "sector_heat": 0.20,      # D 板块热度
    "auction_shape": 0.20,    # E 竞价形态
}


def _to_eltdx_code(symbol: str) -> str | None:
    """内部 '600519.SH' → eltdx 'sh600519'。"""
    if not symbol or "." not in symbol:
        return None
    code, suffix = symbol.split(".", 1)
    return f"{suffix.lower()}{code}"


def _get_auction_provider():
    """获取 eltdx provider (未配置/未安装时返回 None, 不阻塞主流程)。"""
    try:
        from app.data_providers.custom import is_custom_provider, get_provider

        if not is_custom_provider("eltdx"):
            return None
        return get_provider("eltdx")
    except Exception as e:  # noqa: BLE001
        logger.debug("eltdx provider 获取失败: %s", e)
        return None


def _read_auction_series(symbols: list[str], trade_date: date) -> dict[str, dict]:
    """批量读取 eltdx 竞价系列 → {symbol: {auction_price, matched_vol, unmatched_vol, dir}}。

    取 9:25 集合竞价结束点 (09:25:00 前最后一点) 作为竞价撮合结果:
      auction_price   = 竞价撮合价 (≈ 开盘价)
      matched_vol     = 竞价已匹配量 (手)
      unmatched_vol   = 竞价未匹配量 (手)
      unmatched_dir   = 未匹配方向 (1=买压, -1=卖压, 0=无)

    注意: eltdx auctions.series 返回的 points 是一整天快照序列
    (09:15 早盘集合竞价 → 14:57 收盘集合竞价), points[-1] 是收盘前数据,
    不是 9:25 撮合价。必须按时间截取 09:25:00 (time_seconds <= 33900)
    前的最后一点, 才是真正的竞价撮合结果。
    任何失败软返回空 dict, 不阻断评分 (竞价量能/形态维度归 0)。
    """
    provider = _get_auction_provider()
    if provider is None:
        return {}

    # eltdx 插件通过 _get_client 惰性建连; 直接取底层 client
    client = None
    try:
        client = provider._get_client()  # noqa: SLF001
    except Exception:  # noqa: BLE001
        return {}

    # 09:25:00 = 9*3600 + 25*60 = 33900 秒
    _AUCTION_END_SECONDS = 9 * 3600 + 25 * 60

    out: dict[str, dict] = {}
    for symbol in symbols:
        code = _to_eltdx_code(symbol)
        if code is None:
            continue
        try:
            series = client.auctions.series(code, trade_date.isoformat())
            points = getattr(series, "points", None) or ()
            if not points:
                continue
            # 只取早盘集合竞价阶段 (09:25:00 前) 的点, 取最后一点为撮合结果
            last = None
            for p in points:
                ts = getattr(p, "time_seconds", None)
                if ts is None:
                    continue
                if ts <= _AUCTION_END_SECONDS:
                    last = p
            if last is None:
                continue
            price = float(last.price) if getattr(last, "price", None) is not None else None
            matched = int(last.matched_volume) if getattr(last, "matched_volume", None) is not None else 0
            unmatched = int(last.unmatched_volume) if getattr(last, "unmatched_volume", None) is not None else 0
            direction = int(getattr(last, "unmatched_direction_raw", 0) or 0)
            out[symbol] = {
                "auction_price": price,
                "matched_vol": matched,
                "unmatched_vol": unmatched,
                "unmatched_dir": direction,
            }
        except Exception as e:  # noqa: BLE001
            logger.debug("eltdx 竞价读取失败 %s: %s", symbol, e)
            continue
    return out


def _local_trading_days(data_dir: Path) -> list[date]:
    """本地日K分区日期 = 已知交易日集合 (升序), 与 auction_benchmark 同口径。"""
    import re

    root = data_dir / "kline_daily"
    days: list[date] = []
    try:
        for d in root.iterdir():
            m = re.match(r"^date=(\d{4}-\d{2}-\d{2})$", d.name)
            if d.is_dir() and m:
                try:
                    days.append(date.fromisoformat(m.group(1)))
                except ValueError:
                    continue
    except OSError:
        return []
    return sorted(days)


def _prev_trading_day(data_dir: Path, d: date) -> date | None:
    days = _local_trading_days(data_dir)
    earlier = [x for x in days if x < d]
    return max(earlier) if earlier else None


def _next_trading_day(data_dir: Path, d: date) -> date | None:
    days = _local_trading_days(data_dir)
    later = [x for x in days if x > d]
    return min(later) if later else None


def _read_closes(data_dir: Path, d: date | None, columns: list[str]) -> dict[str, dict[str, float]]:
    """某交易日全市场 {symbol: {col: value}}; 无分区/读失败返回空。"""
    if d is None:
        return {}
    root = data_dir / "kline_daily" / f"date={d.isoformat()}"
    try:
        files = sorted(root.glob("*.parquet"))
        if not files:
            return {}
        df = pl.concat([pl.read_parquet(f, columns=["symbol", *columns]) for f in files])
        out: dict[str, dict[str, float]] = {}
        for row in df.iter_rows(named=True):
            out[str(row["symbol"])] = {c: row[c] for c in columns}
        return out
    except (OSError, pl.exceptions.PolarsError):
        return {}


def _load_sector_heat(data_dir: Path, repo, as_of: date) -> dict[str, float]:
    """板块热度 → {concept_name: score}。加载主线排行当日概念分; 失败返回空。

    复用 market_mainline.load_mainline_history 读取主线时序, 取 as_of 当日各概念
    score (热度分)。供候选池按概念归属加分, 缺数据时返回空 (维度归 0)。
    """
    try:
        from app.services.market_mainline import load_mainline_history

        df = load_mainline_history(data_dir, kind="concept")
        if df.is_empty():
            return {}
        if "date" in df.columns:
            df = df.filter(pl.col("date") == as_of)
        if df.is_empty() or "member" not in df.columns:
            return {}
        score_col = "score" if "score" in df.columns else "rank"
        out: dict[str, float] = {}
        for row in df.iter_rows(named=True):
            member = str(row.get("member") or "")
            if not member:
                continue
            out[member] = float(row.get(score_col) or 0)
        return out
    except Exception as e:  # noqa: BLE001
        logger.debug("板块热度加载失败: %s", e)
        return {}


def _load_symbol_concepts(repo, as_of: date) -> dict[str, list[str]]:
    """{symbol: [concept_name]} 映射。复用 rps_rotation._load_concept_map_df。"""
    try:
        from app.services.rps_rotation import _load_concept_map_df  # noqa: PLC2701

        map_df, _ = _load_concept_map_df(repo, kind="concept")
        if map_df.is_empty():
            return {}
        out: dict[str, list[str]] = {}
        sym_col = "_sym_up" if "_sym_up" in map_df.columns else "symbol"
        kind_col = "concept"
        for row in map_df.iter_rows(named=True):
            sym = str(row.get(sym_col) or "").upper()
            member = str(row.get(kind_col) or "")
            if not sym or not member:
                continue
            out.setdefault(sym, []).append(member)
        return out
    except Exception as e:  # noqa: BLE001
        logger.debug("概念映射加载失败: %s", e)
        return {}


def _limit_ratio(symbol: str) -> float:
    """该股涨跌幅限制比例 (0.10 / 0.20 / 0.30)。"""
    code = symbol.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    if code.startswith(("8", "4")):  # 北交所 83/87/43/92 等 8/4 开头
        return 0.30
    return 0.10


def _today_limit_up_price(symbol: str, prev_close: float | None) -> float | None:
    """由昨收推算今日涨停价 (板块涨幅规则)。"""
    if not prev_close or prev_close <= 0:
        return None
    code = symbol.split(".")[0]
    if "ST" in symbol.upper() and not code.startswith(("8", "4")):
        ratio = 0.05
    else:
        ratio = _limit_ratio(symbol)
    return round(prev_close * (1 + ratio), 2)


def _norm_multiplier(symbol: str) -> float:
    """高开归一化倍数 (以主板 10CM 为基准): 10CM=1 / 20CM=2 / 30CM=3。"""
    code = symbol.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 2.0
    if code.startswith(("8", "4")):  # 北交所
        return 3.0
    return 1.0


def _open_position_score(symbol: str, gap_pct: float) -> float:
    """开盘位置子分 (0~100): 高开幅度按涨跌幅限制归一化后分档打分。

    归一化 (算法乙, 以主板 10CM 为基准):
      归一化高开 = gap_pct × 倍数, 其中倍数 10CM=1 / 20CM=2 / 30CM=3。
      20CM/30CM 票单日涨跌幅大, 其高开幅度须按倍数放大才对等。

    分档 (归一化区间 → 分值), 依据 2026 年量化回测与实战复盘:
      3~6   最优黄金区 (收红 87.2%、炸板亏损 <11%)      → 100
      0~3   温和健康, 有分歧空间, 次优                  → 72
      6~8   偏强, 一致性上升, 盘中兑现风险抬升          → 52
      -3~0  良性分歧/弱转强, 可观察, 弱于正高开         → 36
      8~10  极致高开, 大面概率 43.8%, 诱多陷阱          → 20
    区间外 (低开 < -3 或高开 >= 10): 核按钮/一字板风险, → 0
    """
    norm = gap_pct * _norm_multiplier(symbol)  # 归一化高开幅度
    if 3.0 <= norm < 6.0:
        return 100.0
    if 0.0 <= norm < 3.0:
        return 72.0
    if 6.0 <= norm < 8.0:
        return 52.0
    if -3.0 <= norm < 0.0:
        return 36.0
    if 8.0 <= norm < 10.0:
        return 20.0
    return 0.0


def _build_candidates(repo, ref_day: date) -> pl.DataFrame:
    """候选池: 从 ref_day(通常为选股日前一交易日) 的 enriched 数据筛出
    「涨停/炸板/连板」股作为竞价选股候选。

    竞价选股是盘前决策: 9:25 时当日 K 线尚不存在, 候选只能来自昨日收盘后的
    涨停/炸板/连板名单。

    复用 ScreenerService._load_enriched_for_date 读取含完整指标的 enriched 数据。
    筛选条件: signal_limit_up 或 signal_broken_limit_up 或 consecutive_limit_ups > 0。
    """
    from app.services.screener import ScreenerService

    svc = ScreenerService(repo)
    df = svc._load_enriched_for_date(ref_day)  # noqa: SLF001
    if df.is_empty():
        return pl.DataFrame()

    # 保留列 (存在才选)
    keep = ["symbol", "name", "open", "high", "low", "close", "raw_close",
            "volume", "change_pct", "consecutive_limit_ups", "limit_up_price",
            "signal_limit_up", "signal_broken_limit_up"]
    keep = [c for c in keep if c in df.columns]

    # 候选条件: 涨停 or 炸板 or 连板 > 0
    cond = pl.lit(False)
    if "signal_limit_up" in df.columns:
        cond = cond | pl.col("signal_limit_up").fill_null(False)
    if "signal_broken_limit_up" in df.columns:
        cond = cond | pl.col("signal_broken_limit_up").fill_null(False)
    if "consecutive_limit_ups" in df.columns:
        cond = cond | (pl.col("consecutive_limit_ups").fill_null(0) > 0)

    return df.select(keep).filter(cond)


def _score_row(
    row: dict[str, Any],
    auction: dict | None,
    prev_close: float | None,
    sector_heat: dict[str, float],
    concepts: list[str],
    today_limit_up: float | None = None,
    vol_pct: float | None = None,
    today_open: float | None = None,
    symbol: str = "",
) -> tuple[float, dict[str, float], str]:
    """单行五维评分 → (总分, 各维度分, 剔除原因)。

    剔除原因 (非空表示硬过滤命中):
      - one_word: 一字板 (开盘价 >= 涨停判定价)
    各维度分归一化到 0~100, 总分 = Σ(维度分 × 权重) - 风险分, 落到 0~100 区间。

    vol_pct: 该股竞价总量在候选池中的分位 (0~100), 用于量能横向标准化,
             规避 eltdx 竞价量与 kline 成交量单位不一致的风险。
    today_open: 选股日真实开盘价 (日 K open, 9:25 集合竞价撮合价)。
                一字板判断与开盘位置均以此为准; eltdx 竞价价仅为盘前虚拟参考价,
                09:24:57 后仍在变动, 不等于最终开盘价, 不作价格基准。
    """
    auction_price = (auction or {}).get("auction_price")
    limit_up_price = today_limit_up

    # ── 硬过滤: 一字板 ──
    # 开盘价直接顶到今日涨停价 → 开盘即一字板, 无法买入, 剔除
    if (
        today_open is not None
        and limit_up_price is not None
        and limit_up_price > 0
        and today_open >= limit_up_price
    ):
        return 0.0, {}, "one_word"

    dims: dict[str, float] = {}
    matched = (auction or {}).get("matched_vol", 0) or 0
    unmatched = (auction or {}).get("unmatched_vol", 0) or 0
    direction = (auction or {}).get("unmatched_dir", 0) or 0

    # A 竞价量能 (25%): 竞价总量在候选池中的分位映射到 0~100。
    #    横向标准化, 与具体单位无关, 量越大分越高, 平滑无阶梯。
    if vol_pct is not None:
        dims["auction_volume"] = max(0.0, min(100.0, vol_pct))
    else:
        dims["auction_volume"] = 0.0

    # B 开盘位置 (20%): 真实开盘价 vs 昨收高开幅度, 按涨跌幅限制归一化后分档打分。
    #    10CM/20CM/30CM 的高开幅度先 ×倍数 归一化到同一把尺子, 再落入 -3~10 档位表。
    ref_price = today_open if today_open is not None else auction_price
    if ref_price is not None and prev_close and prev_close > 0:
        gap_pct = (ref_price / prev_close - 1.0) * 100
        dims["open_position"] = _open_position_score(symbol, gap_pct)
    else:
        dims["open_position"] = 0.0

    # C 连板位置 (15%): 连续倒 U 形曲线, 3 板最佳, 向两侧平滑衰减。
    consec = int(row.get("consecutive_limit_ups") or 0)
    if consec >= 1:
        # 峰值在 3 板, 1 板约 55 分, 3 板 100 分, 7 板约 28 分
        dims["ladder_position"] = max(0.0, min(100.0, 100.0 * math.exp(-((consec - 3) ** 2) / (2 * 2.0 ** 2))))
    else:
        dims["ladder_position"] = 0.0

    # D 板块热度 (20%): 所属概念中热度最高分
    if concepts and sector_heat:
        best = max((sector_heat.get(c, 0.0) for c in concepts), default=0.0)
        # score 是主线加权分, 取其相对强度 (score 越高越热)
        dims["sector_heat"] = min(100.0, best * 100.0) if best else 0.0
    else:
        dims["sector_heat"] = 0.0

    # E 竞价形态 (20%): 未匹配方向买压加分, 卖压减分; 平衡/无数据中性。
    #    用连续映射: 买压越强分越高 (50→100), 卖压越强分越低 (50→0)。
    if unmatched > 0 and matched > 0:
        ratio = unmatched / (matched + unmatched)
        if direction > 0:  # 买压: 未匹配是买方挂单未成交
            dims["auction_shape"] = 50.0 + ratio * 50.0
        elif direction < 0:  # 卖压
            dims["auction_shape"] = 50.0 - ratio * 50.0
        else:
            dims["auction_shape"] = 50.0
    else:
        dims["auction_shape"] = 0.0

    # 风险分: 炸板/高位连板扣分
    risk = 0.0
    if row.get("signal_broken_limit_up"):
        risk += 1.5
    if consec >= 7:
        risk += 1.0

    raw = sum(dims.get(k, 0.0) * w for k, w in _WEIGHTS.items())
    total = raw - risk  # 0~100 区间
    return max(0.0, total), dims, ""


def _enrich_t1(
    data_dir: Path,
    as_of: date,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """叠加 T+0 与 T+1 真实收益。

    - T+0: 选股日开盘价买入 → 当日收盘价卖出 (as_of 当日 close)。
    - T+1: 选股日开盘价买入 → 次日收盘价卖出 (nxt 日 close)。
    买入价用真实开盘价 (日 K open, 9:30 可成交价); 竞价撮合价仅为盘前决策参考,
    实际成交发生在开盘。对应收盘分区晚到时先给 None, 到了自然补上。
    """
    nxt = _next_trading_day(data_dir, as_of)
    today_closes = _read_closes(data_dir, as_of, ["close"])
    next_closes = _read_closes(data_dir, nxt, ["close"])
    for r in rows:
        buy_price = r.get("open")
        if not buy_price or buy_price <= 0:
            buy_price = r.get("auction_price")
        t0 = None
        t1 = None
        if buy_price and buy_price > 0:
            tc = today_closes.get(r["symbol"], {}).get("close")
            if tc is not None:
                t0 = tc / buy_price - 1.0
            nc = next_closes.get(r["symbol"], {}).get("close")
            if nc is not None:
                t1 = nc / buy_price - 1.0
        r["t0_pct"] = t0
        r["t1_pct"] = t1
    return rows


def _build_result(repo, as_of: date) -> dict[str, Any]:
    """完整竞价选股流程 → 前端容器。"""
    data_dir: Path = repo.store.data_dir
    prev_day = _prev_trading_day(data_dir, as_of)
    if prev_day is None:
        return {
            "state": "no_data",
            "as_of": str(as_of),
            "items": [],
            "total": 0,
            "one_word_filtered": 0,
        }
    candidates = _build_candidates(repo, prev_day)
    if candidates.is_empty():
        return {
            "state": "ok",
            "as_of": str(as_of),
            "items": [],
            "total": 0,
            "one_word_filtered": 0,
        }

    symbols = candidates["symbol"].to_list()
    prev_closes = _read_closes(data_dir, prev_day, ["close"])
    today_opens = _read_closes(data_dir, as_of, ["open"])

    auctions = _read_auction_series(symbols, as_of)
    sector_heat = _load_sector_heat(data_dir, repo, as_of)
    concept_map = _load_symbol_concepts(repo, as_of)

    # 竞价总量分位: 横向标准化量能, 规避单位不一致 (仅对有效竞价量做分位)。
    vol_totals = [
        (auctions.get(s, {}) or {}).get("matched_vol", 0)
        + (auctions.get(s, {}) or {}).get("unmatched_vol", 0)
        for s in symbols
    ]
    vol_totals = [v for v in vol_totals if v > 0]
    vol_pcts: dict[str, float] = {}
    if vol_totals:
        sorted_vols = sorted(vol_totals)
        n = len(sorted_vols)
        for s in symbols:
            a = auctions.get(s, {}) or {}
            v = a.get("matched_vol", 0) + a.get("unmatched_vol", 0)
            if v <= 0:
                vol_pcts[s] = 0.0
                continue
            # 分位 = 低于 v 的数量占比 × 100 (0~100)
            below = sum(1 for x in sorted_vols if x < v)
            vol_pcts[s] = round(below / n * 100.0, 2)

    items: list[dict[str, Any]] = []
    one_word_filtered = 0
    for row in candidates.iter_rows(named=True):
        symbol = str(row["symbol"])
        auction = auctions.get(symbol)
        prev_close = prev_closes.get(symbol, {}).get("close")
        concepts = concept_map.get(symbol.upper(), [])
        today_limit = _today_limit_up_price(symbol, prev_close)

        total, dims, reject = _score_row(
            row, auction, prev_close, sector_heat, concepts, today_limit,
            vol_pct=vol_pcts.get(symbol),
            today_open=today_opens.get(symbol, {}).get("open"),
            symbol=symbol,
        )
        if reject == "one_word":
            one_word_filtered += 1
            continue

        items.append({
            "symbol": symbol,
            "name": row.get("name"),
            "consec": int(row.get("consecutive_limit_ups") or 0),
            "auction_price": (auction or {}).get("auction_price"),
            "open": today_opens.get(symbol, {}).get("open"),
            "change_pct": row.get("change_pct"),
            "matched_vol": (auction or {}).get("matched_vol", 0),
            "unmatched_vol": (auction or {}).get("unmatched_vol", 0),
            "unmatched_dir": (auction or {}).get("unmatched_dir", 0),
            "score": round(total, 2),
            "dims": {k: round(v, 2) for k, v in dims.items()},
            "concepts": concepts[:5],
        })

    # 按评分降序
    items.sort(key=lambda x: x["score"], reverse=True)
    items = _enrich_t1(data_dir, as_of, items)

    return {
        "state": "ok",
        "as_of": str(as_of),
        "items": items,
        "total": len(items),
        "one_word_filtered": one_word_filtered,
    }


def build_router():
    from fastapi import APIRouter, HTTPException
    from fastapi import Query

    router = APIRouter(prefix="/api/custom/jingjia", tags=["custom-jingjia"])

    @router.get("/dates")
    async def dates(request: Request):
        """可用选股日列表 (enriched 日K 已覆盖的交易日, 降序, 最多 60 个)。"""
        repo = getattr(request.app.state, "repo", None)
        if repo is None:
            raise HTTPException(status_code=503, detail="数据仓库未就绪")
        try:
            rows = repo.execute_all(
                "SELECT DISTINCT date FROM kline_enriched "
                "ORDER BY date DESC LIMIT 60"
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("获取选股日列表失败: %s", e)
            raise HTTPException(status_code=500, detail=f"获取选股日列表失败: {e}")
        out: list[str] = []
        for r in rows or []:
            d = r[0] if isinstance(r, (tuple, list)) else r
            try:
                out.append(str(d if not isinstance(d, date) else d.isoformat()))
            except Exception:  # noqa: BLE001
                continue
        return {"dates": out}

    @router.get("/select")
    async def select(
        request: Request,
        as_of: str | None = Query(None, description="选股日期 YYYY-MM-DD, 默认最新交易日"),
    ):
        repo = getattr(request.app.state, "repo", None)
        if repo is None:
            raise HTTPException(status_code=503, detail="数据仓库未就绪")

        target: date | None = None
        if as_of:
            try:
                target = date.fromisoformat(as_of)
            except ValueError:
                raise HTTPException(status_code=400, detail="日期格式应为 YYYY-MM-DD")

        if target is None:
            from app.services.screener import ScreenerService
            target = ScreenerService(repo).latest_date()
            if target is None:
                return {"state": "no_data", "as_of": None, "items": [], "total": 0}

        try:
            return _build_result(repo, target)
        except Exception as e:  # noqa: BLE001
            logger.exception("竞价选股计算失败: %s", e)
            raise HTTPException(status_code=500, detail=f"竞价选股计算失败: {e}")

    return router


def setup(registrar: BackendExtensionRegistrar) -> None:
    registrar.include_router(build_router())


def startup(context: ExtensionContext) -> None:
    # 竞价选股无状态, 无需启动期初始化; 数据在请求期从 app.state 获取。
    _ = context
