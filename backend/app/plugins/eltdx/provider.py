"""ELTDX(通达信 7709 协议开源客户端)内置数据源 provider。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
插件被 loader 扫描注册后, 各 service 无需改动即可路由。

实现数据集:
  - realtime    A 股全市场快照(all_a_shares 枚举 + get_snapshots 批量) + 指数快照
  - daily       A 股日K(原始价, count 模式单标的拉取)
  - minute      分钟K(1m, 240 根/日, 原始价)
  - depth5      五档盘口(quotes.get_depth, 买/卖各 5 档 price+volume, 单位手)
  - full_minute 全量分钟(分钟修复轮, minutes.history 当日 240 根, 单位手)
未声明 adj_factor / financial → provider_has_dataset 为 False, 回退 tickflow。

单位与口径 (CONTRIBUTING §3, 2026-10 实测):
  - 快照 total_hand 已是手, 直接透传(floor); amount 已是元。
  - 快照无涨跌幅字段 → change_pct = (last-pre_close)/pre_close, 小数制。
  - 日K volume_lots 已是手(带 .8 尾数 → floor); amount 已是元; time 带 +08:00 时区。
  - 分钟K point.volume 已是手(实测全天 sum = 日K volume_lots); datetime 北京墙钟。
  - 代码格式: eltdx 返回 'sh600000'/'sz000001' → 转 '600000.SH'/'000001.SZ'。
"""

from __future__ import annotations

import contextlib
import logging
import math
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

logger = logging.getLogger(__name__)

# 只声明真实提供的数据集; 其余数据集 provider_has_dataset 返回 False → 回退 tickflow
_DATASETS = ("realtime", "daily", "minute", "depth5", "full_minute")

_BEIJING = ZoneInfo("Asia/Shanghai")

_DAILY_SCHEMA = {
    "symbol": pl.String, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "volume": pl.Int64, "amount": pl.Float64,
}
_MINUTE_SCHEMA = {
    "symbol": pl.String, "datetime": pl.Datetime("us"),
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "volume": pl.Float64, "amount": pl.Float64,
}

_HIST_INTERVAL_S = 0.05  # 单标的日K/分钟请求节流(本机协议, 无服务端限频, 可放宽)
_MINUTE_BARS_PER_DAY = 240

# 实测 2026-10: get_snapshots 单次最多返回 80 条(超出的请求被服务端截断到 80);
# 超过约 1000 只直接 TCP 断流。故全市场快照按 80 只/批分批拉取。
_SNAPSHOT_BATCH = 80


def _module_available() -> bool:
    try:
        import importlib.util

        return importlib.util.find_spec("eltdx") is not None
    except Exception:  # noqa: BLE001
        return False


def availability() -> tuple[bool, str]:
    """loader 启动自检: eltdx 已安装即注册(无需 API Key)。不抛异常。"""
    if not _module_available():
        return False, "eltdx 未安装(可 pip install eltdx 或 uv add eltdx)"
    return True, "ok"


@dataclass
class _EltdxConfig:
    """轻量 config shim, 让 custom loader 的 provider_has_dataset 能识别本 provider。"""

    name: str = "eltdx"
    display_name: str = "ELTDX"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(raw: str) -> str | None:
    """eltdx 代码 'sh600000'/'sz000001' → 内部 '600000.SH'/'000001.SZ'。"""
    if not raw:
        return None
    raw = str(raw).strip().lower()
    if len(raw) < 7:
        return None
    exchange = raw[:2]  # sh / sz / bj
    code = raw[2:8]
    if not code.isdigit():
        return None
    suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(exchange, exchange.upper())
    return f"{code}.{suffix}"


def _to_eltdx_code(symbol: str) -> str | None:
    """内部 '600519.SH' → eltdx 'sh600519'。"""
    if not symbol or "." not in symbol:
        return None
    code, suffix = symbol.split(".", 1)
    exchange = suffix.lower()  # SH → sh, SZ → sz, BJ → bj
    return f"{exchange}{code}"


class EltdxProvider:
    """ELTDX 数据源。realtime = 全市场 A 股 + 指数批量快照; daily/minute 单标的拉取。"""

    name = "eltdx"
    builtin = True

    def __init__(self) -> None:
        self.config = _EltdxConfig()
        self._client = None

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _get_client(self):
        if self._client is None:
            import eltdx

            self._client = eltdx.TdxClient()
            self._client.connect()
        return self._client

    # ---- realtime ----
    def get_realtime(self) -> list[dict]:
        """全市场 A 股实时快照 → 内部 realtime records。失败软返回空列表(不阻断轮询)。"""
        client = self._get_client()
        try:
            universe = client.codes.all_a_shares()
        except Exception as e:  # noqa: BLE001
            logger.warning("ELTDX 股票池枚举失败: %s", e)
            return []

        fallback_ms = int(time.time() * 1000)
        records: list[dict] = []
        failed_batches = 0
        for i in range(0, len(universe), _SNAPSHOT_BATCH):
            codes = universe[i : i + _SNAPSHOT_BATCH]
            try:
                snaps = client.quotes.get_snapshots(codes)
            except Exception as e:  # noqa: BLE001
                failed_batches += 1
                logger.warning("ELTDX 快照批次失败(第 %d 批, %d 只): %s",
                               i // _SNAPSHOT_BATCH + 1, len(codes), e)
                continue
            for s in snaps:
                rec = _map_snapshot(s, fallback_ms)
                if rec is not None:
                    records.append(rec)
        if failed_batches:
            logger.warning("ELTDX 快照 %d/%d 批失败, 本轮仅返回 %d 条",
                           failed_batches, math.ceil(len(universe) / _SNAPSHOT_BATCH), len(records))
        if not records and universe:
            logger.warning("ELTDX 实时行情本轮 0 条(请求 %d 只), 疑似连接或协议异常", len(universe))
        else:
            logger.info("ELTDX 实时行情拉取完成: %d 条(请求 %d 只)", len(records), len(universe))
        return records

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照。失败返回 None 保留上轮缓存, 成功无数据返回 []。"""
        codes = []
        for s in symbols:
            c = _to_eltdx_code(s)
            if c and c not in codes:
                codes.append(c)
        if not codes:
            return []
        try:
            snaps = self._get_client().quotes.get_snapshots(codes)
        except Exception as e:  # noqa: BLE001
            logger.warning("ELTDX 指数行情拉取失败: %s", e)
            return None
        fallback_ms = int(time.time() * 1000)
        records = [r for r in (_map_snapshot(s, fallback_ms) for s in snaps) if r]
        logger.info("ELTDX 指数行情拉取完成: %d 条(请求 %d 只)", len(records), len(codes))
        return records

    # ---- daily ----
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        chunks = [
            df
            for df in self.iter_daily(
                symbols, start_time=start_time, end_time=end_time,
                asset_type=asset_type, on_chunk_done=on_chunk_done,
            )
            if not df.is_empty()
        ]
        return pl.concat(chunks, how="diagonal_relaxed") if chunks else pl.DataFrame(schema=_DAILY_SCHEMA)

    def iter_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """逐标的 count 模式拉日K → 内部契约(原始价, volume 手, amount 元)。"""
        if not symbols or asset_type != "stock":
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        client = self._get_client()
        start_d = start_time.date() if start_time else None
        end_d = end_time.date() if end_time else date.today()
        # count 模式: 窗口自然日 → 根数(交易日≈自然日×5/7, 加冗余)
        span = (end_d - start_d).days if start_d else 4000
        count = max(30, min(3000, int(span * 5 / 7) + 20))

        for i, symbol in enumerate(symbols):
            code = _to_eltdx_code(symbol)
            rows: list[dict] = []
            if code is not None:
                try:
                    series = client.bars.get(code, period="day", count=count)
                    rows = _kline_rows(symbol, series, start_d, end_d)
                except Exception as e:  # noqa: BLE001
                    logger.warning("ELTDX 日K %s 拉取失败: %s", symbol, e)
            yield pl.DataFrame(rows, schema=_DAILY_SCHEMA)
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            time.sleep(_HIST_INTERVAL_S)

    # ---- minute ----
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols or asset_type != "stock" or freq != "1m":
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        client = self._get_client()
        out: list[pl.DataFrame] = []
        for i, symbol in enumerate(symbols):
            code = _to_eltdx_code(symbol)
            rows: list[dict] = []
            if code is not None:
                try:
                    # minutes.history 需要具体交易日; 无 end_time 时用最近交易日
                    day = (end_time or datetime.now()).date()
                    series = client.minutes.history(code, day.isoformat())
                    rows = _minute_rows(symbol, series, start_time, end_time)
                except Exception as e:  # noqa: BLE001
                    logger.warning("ELTDX 分钟K %s 拉取失败: %s", symbol, e)
            out.append(pl.DataFrame(rows, schema=_MINUTE_SCHEMA))
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            time.sleep(_HIST_INTERVAL_S)
        parts = [df for df in out if not df.is_empty()]
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame(schema=_MINUTE_SCHEMA)

    # ---- depth5 ----
    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """五档盘口 → {symbol: 标准盘口字典}。单位手(透传), 价格 float。

        契约对齐 depth_service._seal_locked: 消费 ask_volumes[0]/bid_volumes[0]
        (一档量) 判真假封板, timestamp(毫秒 epoch) 记拉取时刻。eltdx get_depth
        返回 QuoteRefreshPage, 每条 QuoteRefreshRecord 含 buy_levels/sell_levels
        各 5 档 QuoteLevel(price, volume), volume 单位手(2026-10 与日K对拍验证)。
        """
        if not symbols:
            return {}
        client = self._get_client()
        fallback_ms = int(time.time() * 1000)
        out: dict[str, dict] = {}
        for i in range(0, len(symbols), _SNAPSHOT_BATCH):
            codes = [_to_eltdx_code(s) for s in symbols[i : i + _SNAPSHOT_BATCH]]
            codes = [c for c in codes if c]
            if not codes:
                continue
            try:
                page = client.quotes.get_depth(codes)
            except Exception as e:  # noqa: BLE001
                logger.warning("ELTDX 五档批次失败(第 %d 批, %d 只): %s",
                               i // _SNAPSHOT_BATCH + 1, len(codes), e)
                continue
            for rec in getattr(page, "records", ()) or ():
                entry = _depth_entry(rec, fallback_ms)
                if entry is not None:
                    out[entry["symbol"]] = entry
        return out

    # ---- full_minute ----
    def get_intraday_batch(
        self, symbols: list[str], count: int = 300, asset_type: str = "stock",
    ) -> pl.DataFrame:
        """全量分钟修复轮: 当日窗口逐标的拉取全天 1m 分钟(240 根)。

        与 get_minute 同口径(volume 手, datetime 北京墙钟 naive), 复用
        minutes.history 当日窗口; count 参数为接口一致性保留, eltdx 按自然日
        取全天无需根数。稳态增量(get_intraday_latest)未实现 → minute_refresh
        自动降级为仅修复轮模式(节奏下限 60s)。
        """
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        client = self._get_client()
        today = (datetime.now()).date()
        out: list[pl.DataFrame] = []
        for symbol in symbols:
            code = _to_eltdx_code(symbol)
            if code is None:
                continue
            try:
                series = client.minutes.history(code, today.isoformat())
                rows = _minute_rows(symbol, series, None, None)
            except Exception as e:  # noqa: BLE001
                logger.warning("ELTDX 全量分钟 %s 拉取失败: %s", symbol, e)
                continue
            if rows:
                out.append(pl.DataFrame(rows, schema=_MINUTE_SCHEMA))
            time.sleep(_HIST_INTERVAL_S)
        parts = [df for df in out if not df.is_empty()]
        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame(schema=_MINUTE_SCHEMA)

    # ---- 设置页试拉 ----
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        if dataset == "daily":
            syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
            try:
                df = self.get_daily(syms, datetime.now() - timedelta(days=30), datetime.now())
            except Exception as e:  # noqa: BLE001
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            head = df.head(5).to_dicts()
            for row in head:
                for k, v in list(row.items()):
                    if isinstance(v, (date, datetime)):
                        row[k] = v.isoformat()
            return {"provider": self.name, "dataset": dataset, "rows": df.height,
                    "columns": df.columns, "preview": head}
        if dataset == "minute":
            syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
            try:
                end = datetime.now()
                df = self.get_minute(syms, end - timedelta(days=3), end)
            except Exception as e:  # noqa: BLE001
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            head = df.head(5).to_dicts()
            for row in head:
                for k, v in list(row.items()):
                    if isinstance(v, (date, datetime)):
                        row[k] = v.isoformat()
            return {"provider": self.name, "dataset": dataset, "rows": df.height,
                    "columns": df.columns, "preview": head}
        if dataset == "realtime":
            syms = [s for s in (symbols or [])][:5] or ["000001.SZ", "600000.SH"]
            try:
                codes = [c for c in (_to_eltdx_code(s) for s in syms) if c]
                snaps = self._get_client().quotes.get_snapshots(codes)
            except Exception as e:  # noqa: BLE001
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            fallback_ms = int(time.time() * 1000)
            head = [r for r in (_map_snapshot(s, fallback_ms) for s in snaps) if r]
            return {"provider": self.name, "dataset": dataset, "rows": len(snaps),
                    "columns": list(head[0].keys()) if head else ["symbol", "last_price"],
                    "preview": head[:5]}
        if dataset == "depth5":
            syms = [s for s in (symbols or [])][:5] or ["000001.SZ", "600000.SH"]
            try:
                data = self.get_depth_batch(syms)
            except Exception as e:  # noqa: BLE001
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            preview = []
            for sym, v in list(data.items())[:3]:
                preview.append({
                    "symbol": sym,
                    "bid_prices": v.get("bid_prices"),
                    "ask_prices": v.get("ask_prices"),
                    "bid_volumes": v.get("bid_volumes"),
                    "ask_volumes": v.get("ask_volumes"),
                })
            return {"provider": self.name, "dataset": dataset, "rows": len(data),
                    "columns": ["symbol", "bid_prices", "ask_prices", "bid_volumes", "ask_volumes"],
                    "preview": preview}
        if dataset == "full_minute":
            syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
            try:
                df = self.get_intraday_batch(syms)
            except Exception as e:  # noqa: BLE001
                return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
            head = df.head(5).to_dicts()
            for row in head:
                for k, v in list(row.items()):
                    if isinstance(v, (date, datetime)):
                        row[k] = v.isoformat()
            return {"provider": self.name, "dataset": dataset, "rows": df.height,
                    "columns": df.columns, "preview": head}
        return {"provider": self.name, "dataset": dataset, "rows": 0,
                "error": f"eltdx 插件未接入 {dataset} 数据集(自动回退 TickFlow)"}


def _map_snapshot(snap, fallback_ms: int) -> dict | None:
    """ELTDX QuoteSnapshot → 内部 realtime record。字段按口径透传, 不伪造。

    实测 2026-10: change/change_pct 已是涨跌额(元)/涨跌幅(小数制), 直接透传;
    total_hand 已是手, amount 已是元; full_code = 'sh689009' 用于归一化 symbol。
    """
    last = _to_float(snap.last_price)
    prev = _to_float(snap.pre_close_price)
    if last is None and prev is None:
        return None
    change_amount = _to_float(getattr(snap, "change", None))
    if change_amount is None and last is not None and prev is not None:
        change_amount = last - prev
    change_pct = _to_float(getattr(snap, "change_pct", None))
    if change_pct is None and change_amount is not None and prev:
        change_pct = change_amount / prev

    volume = _to_float(snap.total_hand)  # 已是手
    amount = _to_float(snap.amount)  # 已是元

    symbol = _normalize_symbol(getattr(snap, "full_code", None) or f"{snap.exchange}{snap.code}")

    return {
        "symbol": symbol,
        "name": getattr(snap, "name", None),  # 快照无名称, 由维表关联(实际 eltdx 快照无 name)
        "last_price": last,
        "prev_close": prev,
        "open": _to_float(snap.open_price),
        "high": _to_float(snap.high_price),
        "low": _to_float(snap.low_price),
        "volume": math.floor(volume) if volume is not None else None,
        "amount": amount,
        "change_pct": change_pct,
        "change_amount": change_amount,
        "amplitude": None,  # 快照未提供, 不启发式计算
        "turnover_rate": None,  # 需股本口径, 交给 enriched 管道
        "timestamp": fallback_ms,  # eltdx time_raw 非标准时间戳, 退本地时间
        "session": None,
    }


def _depth_entry(rec, fallback_ms: int) -> dict | None:
    """ELTDX QuoteRefreshRecord → 内部五档盘口字典(契约见 depth_service)。

    buy_levels/sell_levels 各为 5 档 QuoteLevel(price, volume), volume 单位手
    (与日K volume_lots 同口径, 2026-10 实测对拍)。price 转 float, volume 转 int。
    timestamp 用拉取时刻(eltdx 无标准 epoch 时间戳, update_time_raw 为通达信
    HHMMSS 墙钟, 不适用于 age 计算)。
    """
    def _prices(levels) -> list[float]:
        return [float(lv.price) for lv in levels if _to_float(lv.price) is not None]

    def _volumes(levels) -> list[int]:
        out: list[int] = []
        for lv in levels:
            f = _to_float(lv.volume)
            out.append(math.floor(f) if f is not None else 0)
        return out

    buy = getattr(rec, "buy_levels", None) or ()
    sell = getattr(rec, "sell_levels", None) or ()
    bid_prices = _prices(buy)
    ask_prices = _prices(sell)
    if not bid_prices or not ask_prices:
        return None
    symbol = _normalize_symbol(
        getattr(rec, "full_code", None) or f"{getattr(rec, 'exchange', '')}{getattr(rec, 'code', '')}"
    )
    if symbol is None:
        return None
    return {
        "symbol": symbol,
        "bid_prices": bid_prices,
        "bid_volumes": _volumes(buy),
        "ask_prices": ask_prices,
        "ask_volumes": _volumes(sell),
        "timestamp": fallback_ms,
    }


def _kline_rows(symbol: str, series, start_d: date | None, end_d: date) -> list[dict]:
    """ELTDX KlineSeries → 内部日K行(窗口过滤, volume 手, amount 元, time 去时区)。"""
    rows: list[dict] = []
    for bar in series.bars:
        # bar.time 是带 +08:00 的 datetime, 取 date 部分
        d = bar.time.date() if bar.time else None
        if d is None or d > end_d:
            continue
        if start_d is not None and d < start_d:
            continue
        volume = _to_float(bar.volume_lots)  # 已是手
        rows.append({
            "symbol": symbol,
            "date": d,
            "open": _to_float(bar.open),
            "high": _to_float(bar.high),
            "low": _to_float(bar.low),
            "close": _to_float(bar.close),
            "volume": math.floor(volume) if volume is not None else None,
            "amount": _to_float(bar.amount),  # 已是元
        })
    return rows


def _minute_rows(
    symbol: str, series, start_dt: datetime | None, end_dt: datetime | None,
) -> list[dict]:
    """ELTDX MinuteSeries → 内部分钟行(datetime 北京墙钟 naive, volume 手, amount 元)。"""
    rows: list[dict] = []
    for p in series.points:
        # p.time 带 +08:00 时区, 转北京墙钟 naive
        t = p.time
        if t is None:
            continue
        if t.tzinfo is not None:
            t = t.astimezone(_BEIJING).replace(tzinfo=None)
        if start_dt is not None and t < start_dt:
            continue
        if end_dt is not None and t > end_dt:
            continue
        close = _to_float(p.price)
        if close is None:
            continue  # 无成交分钟, 不伪造
        rows.append({
            "symbol": symbol,
            "datetime": t,
            "open": close,  # eltdx 分钟 point 无独立 OHLC, 以 price 为 close(分时口径)
            "high": close,
            "low": close,
            "close": close,
            "volume": _to_float(p.volume),  # 已是手
            "amount": None,  # 分钟成交额 eltdx 未提供, 不伪造
        })
    return rows


__all__ = [
    "EltdxProvider",
    "availability",
]
