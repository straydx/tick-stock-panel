"""AxData(多源量化数据库框架)内置数据源 provider。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
插件被 loader 扫描注册后, 各 service 无需改动即可路由。

实现数据集:
  - daily  A 股日K(原始价, stock_kline_daily_tdx 单标的拉取)
未声明 realtime / minute / adj_factor / depth5 / financial
  → provider_has_dataset 为 False, 回退 tickflow。

单位与口径 (CONTRIBUTING §3, 2026-10 实测):
  - stock_kline_daily_tdx 返回 pandas DataFrame, 列:
    instrument_id / symbol / tdx_code / exchange / trade_time / period /
    open / high / low / close / volume / amount
  - symbol 列已是 '000001.SZ' 格式(带后缀), 直接透传。
  - volume 已是手(实测 000001.SZ 某日 1111850.8, 与 eltdx 同口径), floor。
  - amount 已是元(实测 1281599104.0, 无需换算)。
  - trade_time 带 +08:00 时区 → 取 date 部分, 去时区。
  - 代码入参需 '000001.SZ' 格式(内部格式, 直接透传即可)。
"""

from __future__ import annotations

import contextlib
import logging
import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import polars as pl

logger = logging.getLogger(__name__)

# 只声明真实提供的数据集; 其余数据集 provider_has_dataset 返回 False → 回退 tickflow
_DATASETS = ("daily",)

_DAILY_SCHEMA = {
    "symbol": pl.String, "date": pl.Date,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "volume": pl.Int64, "amount": pl.Float64,
}

_HIST_INTERVAL_S = 0.05  # 单标的日K请求节流(本地库, 无服务端限频)


def _module_available() -> bool:
    try:
        import importlib.util

        return importlib.util.find_spec("axdata") is not None
    except Exception:  # noqa: BLE001
        return False


def availability() -> tuple[bool, str]:
    """loader 启动自检: axdata 已安装即注册(无需 API Key)。不抛异常。"""
    if not _module_available():
        return False, "axdata 未安装(可 pip install axdata 或 uv add axdata)"
    return True, "ok"


@dataclass
class _AxDataConfig:
    """轻量 config shim, 让 custom loader 的 provider_has_dataset 能识别本 provider。"""

    name: str = "axdata"
    display_name: str = "AxData"
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


def _to_date(value) -> date | None:
    """trade_time(pandas Timestamp 带时区) → date, 去时区只取日历日。非法返回 None。"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.date()
        # pandas Timestamp 有 .date() 方法; 兜底转 datetime
        dt = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
        if isinstance(dt, datetime):
            return dt.date()
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except (TypeError, ValueError):
        return None


class AxDataProvider:
    """AxData 数据源。仅 daily(日K历史, 单标的 pandas DataFrame 拉取)。"""

    name = "axdata"
    builtin = True

    def __init__(self) -> None:
        self.config = _AxDataConfig()
        self._client = None

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _get_client(self):
        if self._client is None:
            import axdata

            self._client = axdata.AxDataClient()
        return self._client

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
        """逐标的 stock_kline_daily_tdx 拉日K → 内部契约(原始价, volume 手, amount 元)。"""
        if not symbols or asset_type != "stock":
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        client = self._get_client()
        start_d = start_time.date() if start_time else None
        end_d = end_time.date() if end_time else date.today()

        for i, symbol in enumerate(symbols):
            rows: list[dict] = []
            try:
                # AxData 代码入参即内部 '000001.SZ' 格式, 直接透传; adjust=None 锁原始价
                df = client.call("stock_kline_daily_tdx", code=symbol, adjust=None)
                rows = _kline_rows(symbol, df, start_d, end_d)
            except Exception as e:  # noqa: BLE001
                logger.warning("AxData 日K %s 拉取失败: %s", symbol, e)
            yield pl.DataFrame(rows, schema=_DAILY_SCHEMA)
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            if i < len(symbols) - 1:
                import time

                time.sleep(_HIST_INTERVAL_S)

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
        return {"provider": self.name, "dataset": dataset, "rows": 0,
                "error": f"axdata 插件未接入 {dataset} 数据集(自动回退 TickFlow)"}


def _kline_rows(symbol: str, df, start_d: date | None, end_d: date) -> list[dict]:
    """AxData 日K pandas DataFrame → 内部日K行(窗口过滤, volume 手 floor, amount 元)。"""
    rows: list[dict] = []
    if df is None:
        return rows
    try:
        records = df.to_dict("records")
    except Exception:  # noqa: BLE001 — 非 DataFrame 时兜底空
        return rows
    for r in records:
        d = _to_date(r.get("trade_time"))
        if d is None or d > end_d:
            continue
        if start_d is not None and d < start_d:
            continue
        volume = _to_float(r.get("volume"))
        rows.append({
            "symbol": symbol,
            "date": d,
            "open": _to_float(r.get("open")),
            "high": _to_float(r.get("high")),
            "low": _to_float(r.get("low")),
            "close": _to_float(r.get("close")),
            "volume": math.floor(volume) if volume is not None else None,
            "amount": _to_float(r.get("amount")),  # 已是元
        })
    return rows


__all__ = [
    "AxDataProvider",
    "availability",
]
