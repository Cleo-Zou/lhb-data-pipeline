#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股龙虎榜每日数据获取与邮件推送。

数据源: akshare `stock_lhb_detail_em`（东方财富，免费、无需 Token）。
相比新浪 `stock_lhb_detail_daily_sina`，东方财富的日榜字段完整得多——
自带「涨跌幅」，且「上榜原因」对创业板/科创板的 20% 板股票也完整给出
（新浪接口对这些票返回 NaN），另附龙虎榜净买额、换手率、流通市值等。

每个交易日的数据首次抓取后落成 data/lhb_YYYYMMDD.parquet，之后复用不再重复请求；
非交易日记进 data/_no_data.json，同样不再重复问。
再由全部历史 parquet 生成自包含的 output/index.html，部署到 GitHub Pages，
邮件正文放当日榜单表格 + 看板链接。每次运行顺带跑一遍数据自检，异常会写进邮件。

用法:
    python fetch_lhb.py                    # 抓取昨天（北京时间）的数据
    python fetch_lhb.py --date 2026-08-21  # 抓取指定日期（两种格式都认）
    python fetch_lhb.py --backfill 30      # 回补最近 30 天，攒历史（按范围批量）
    python fetch_lhb.py --no-email         # 抓取 + 生成看板，不发信
    python fetch_lhb.py --email-only --page-url https://…   # 只发信（Pages 部署后调用）
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------- 配置常量 --

MAX_RETRIES = 3           # 接口最大重试次数
RETRY_DELAY = 5           # 重试间隔（秒）
BACKFILL_DELAY = 1.0      # 回补时每次「范围请求」之间的间隔，避免把数据源打急
BACKFILL_CHUNK = 90       # 回补时每段范围的天数（东方财富支持一次取一个日期范围）
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"      # GitHub Pages 部署目录
CST = timezone(timedelta(hours=8))   # 北京时间，中国无夏令时，固定偏移即可

# 摘要中优先展示、按它排序的金额字段（东方财富有「龙虎榜净买额」，最接近「机构净额」）
AMOUNT_COL_CANDIDATES = ("龙虎榜净买额", "龙虎榜成交额", "换手率")

# 看板/邮件正文展示的列（顺序即展示顺序）
DISPLAY_COLS = ["股票代码", "股票名称", "收盘价", "涨跌幅",
                "龙虎榜净买额", "换手率",
                "净买额占总成交比", "成交额占总成交比",
                "流通市值", "市场总成交额",
                "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日",
                "上榜原因"]

# 自检必查的列：任何一个 parquet 缺了这些列就算异常
REQUIRED_COLS = ("股票代码", "股票名称", "收盘价", "涨跌幅", "龙虎榜净买额", "上榜原因")

# 东方财富原始列名 -> 本项目统一列名（其余同名列原样保留）
COLUMN_MAP = {"代码": "股票代码", "名称": "股票名称"}

# 落盘时保留的列（展示列 + 买入/卖出/成交额 + 占比/市值/上榜后表现，附件 parquet 里最完整）
KEEP_COLS = ["股票代码", "股票名称", "收盘价", "涨跌幅",
             "龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "龙虎榜成交额",
             "换手率", "流通市值", "市场总成交额",
             "净买额占总成交比", "成交额占总成交比",
             "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日",
             "上榜原因"]

EXIT_OK = 0
EXIT_FETCH_FAILED = 1


def setup_logging() -> None:
    """配置日志：输出到 stdout，GitHub Actions 可直接查看。"""
    # Windows 控制台默认 GBK，会导致中文日志抛 UnicodeEncodeError
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="获取A股龙虎榜数据并邮件推送")
    parser.add_argument(
        "--date", default=None, metavar="YYYYMMDD",
        help="指定交易日期（YYYYMMDD 或 YYYY-MM-DD），默认为北京时间的昨天",
    )
    parser.add_argument(
        "--backfill", type=int, default=0, metavar="N",
        help="回补最近 N 个自然日的数据（已存在的跳过），用于首次攒历史",
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="只抓取、生成页面，不发送邮件（本地调试用）",
    )
    parser.add_argument(
        "--email-only", action="store_true",
        help="不抓取也不生成页面，只发邮件（Actions 里在 Pages 部署完成后调用）",
    )
    parser.add_argument(
        "--page-url", default=None, metavar="URL",
        help="看板地址，写进邮件正文；也可用环境变量 PAGE_URL",
    )
    return parser.parse_args()


def resolve_date(explicit: str | None) -> str:
    """确定目标日期。默认取昨天——交易日数据次日才完整。

    接受 YYYYMMDD 与 YYYY-MM-DD 两种写法。
    """
    if explicit:
        raw = explicit.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y%m%d")
            except ValueError:
                continue
        raise ValueError(raw)
    return (datetime.now(CST) - timedelta(days=1)).strftime("%Y%m%d")


def parquet_path(date_str: str) -> Path:
    return DATA_DIR / f"lhb_{date_str}.parquet"


# 非交易日不会产生 parquet，不记一笔的话回补时会反复请求周末和节假日
NO_DATA_LEDGER = DATA_DIR / "_no_data.json"


def load_no_data() -> set[str]:
    if not NO_DATA_LEDGER.exists():
        return set()
    try:
        return set(json.loads(NO_DATA_LEDGER.read_text(encoding="utf-8")))
    except Exception as exc:                          # noqa: BLE001
        logging.warning("读取 %s 失败，当作空台账：%s", NO_DATA_LEDGER.name, exc)
        return set()


def save_no_data(dates: set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    NO_DATA_LEDGER.write_text(
        json.dumps(sorted(dates), indent=0), encoding="utf-8")


# ---------------------------------------------------------------- 数据获取 --

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """统一列名、清洗关键字段。"""
    df = df.rename(columns=COLUMN_MAP)
    # 股票代码保持 6 位字符串，前导零不能丢（如 000065）
    if "股票代码" in df.columns:
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    # 上榜日：东方财富给的是 datetime.date，统一成 YYYYMMDD 字符串
    # （单日文件写盘前会丢弃这列，只有范围回补需要它来分组）
    if "上榜日" in df.columns:
        df["上榜日"] = df["上榜日"].apply(
            lambda d: d.strftime("%Y%m%d") if hasattr(d, "strftime")
            else str(d).replace("-", ""))
    # 只保留需要落盘的列（上榜日仅用于范围回补分组，写盘前丢弃）
    keep = [c for c in KEEP_COLS if c in df.columns]
    if "上榜日" in df.columns:
        keep.append("上榜日")
    return df[keep]


def _fetch_range(d0: str, d1: str) -> pd.DataFrame | None:
    """抓取 [d0, d1] 日期范围内的龙虎榜明细，失败重试。

    返回统一列名后的 DataFrame；该范围无数据（纯周末/节假日）返回空 DataFrame；
    重试耗尽仍失败返回 None。
    """
    import akshare as ak   # 延迟导入：import akshare 本身耗时约数秒

    label = d0 if d0 == d1 else f"{d0}~{d1}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info("正在获取 %s 龙虎榜数据（第 %d/%d 次尝试）…",
                         label, attempt, MAX_RETRIES)
            df = ak.stock_lhb_detail_em(start_date=d0, end_date=d1)
            if df is None or len(df) == 0:
                logging.info("%s 无数据表（非交易日或该范围无上榜）", label)
                return pd.DataFrame()
            logging.info("接口返回 %d 行", len(df))
            return _normalize(df)
        except TypeError as exc:
            # 纯非交易日时东方财富接口内部对空结果取下标会抛
            # TypeError("'NoneType' object is not subscriptable")，确定性无数据。
            logging.info("%s 无数据（非交易日），接口抛 TypeError(%s)", label, exc)
            return pd.DataFrame()
        except Exception as exc:                      # noqa: BLE001 - 接口异常类型不确定
            logging.warning("第 %d 次获取失败: %s: %s",
                            attempt, type(exc).__name__, exc)
            if attempt < MAX_RETRIES:
                logging.info("%d 秒后重试…", RETRY_DELAY)
                time.sleep(RETRY_DELAY)

    logging.error("重试 %d 次后仍无法获取 %s 的数据", MAX_RETRIES, label)
    return None


def fetch_lhb(date_str: str) -> pd.DataFrame | None:
    """抓取单日（用于每日增量）。"""
    return _fetch_range(date_str, date_str)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """清洗：去全空行、去重、清理字符串空白、填充文本缺失值。"""
    before = len(df)
    df = df.dropna(how="all").copy()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

    # 股票代码缺失或为空的整行直接丢弃
    if "股票代码" in df.columns:
        df = df[df["股票代码"].astype(str).str.strip().ne("")]

    df = df.drop_duplicates().reset_index(drop=True)

    if len(df) != before:
        logging.info("清洗：%d 行 -> %d 行", before, len(df))
    return df


def load_or_fetch(date_str: str) -> pd.DataFrame | None:
    """优先读本地 parquet；没有才去抓，抓到后落盘。

    返回 None 表示抓取失败；返回空 DataFrame 表示当日确实无数据。
    """
    path = parquet_path(date_str)
    if path.exists():
        df = pd.read_parquet(path)
        logging.info("命中本地缓存 %s（%d 条），跳过抓取", path.name, len(df))
        return df

    df = fetch_lhb(date_str)
    if df is None or df.empty:
        return df

    df = clean(df).drop(columns=[c for c in ("上榜日",) if c in df.columns])
    if df.empty:
        logging.info("清洗后无有效数据，不保存")
        return df

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logging.info("已保存 %d 条数据到 %s（%.1f KB）",
                 len(df), path.name, path.stat().st_size / 1024)
    return df


def backfill(days: int) -> None:
    """回补最近 N 个自然日，按范围批量抓取（东方财富支持一次取一个日期范围）。

    已覆盖的日期（有 parquet 或记进台账的）整段跳过，只对缺口分段请求；
    段内未返回的日期记为 no_data，下次直接跳过。
    """
    logging.info("=== 开始回补最近 %d 天 ===", days)
    no_data = load_no_data()
    # 终点取昨天：今天(交易日)的龙虎榜要到盘后才发布，此刻抓会误判为「无数据」
    end = (datetime.now(CST) - timedelta(days=1)).date()
    start = end - timedelta(days=days)

    fetched = skipped = failed = 0
    cursor = start

    try:
        while cursor <= end:
            c1 = min(cursor + timedelta(days=BACKFILL_CHUNK - 1), end)

            # 段内每个日期都已覆盖则整段跳过
            all_covered = True
            d = cursor
            while d <= c1:
                ds = d.strftime("%Y%m%d")
                if not parquet_path(ds).exists() and ds not in no_data:
                    all_covered = False
                    break
                d += timedelta(days=1)
            if all_covered:
                skipped += (c1 - cursor).days + 1
                cursor = c1 + timedelta(days=1)
                continue

            df = _fetch_range(cursor.strftime("%Y%m%d"), c1.strftime("%Y%m%d"))
            if df is None:
                failed += 1
            elif df.empty:
                d = cursor
                while d <= c1:
                    no_data.add(d.strftime("%Y%m%d"))
                    d += timedelta(days=1)
            else:
                covered: set[str] = set()
                for ds, sub in df.groupby("上榜日"):
                    sub = clean(sub.drop(columns=["上榜日"]).reset_index(drop=True))
                    if sub.empty:
                        continue
                    DATA_DIR.mkdir(parents=True, exist_ok=True)
                    sub.to_parquet(parquet_path(ds), index=False)
                    covered.add(ds)
                    fetched += 1
                # 段内没出现的日期 = 非交易日，记进台账
                d = cursor
                while d <= c1:
                    ds = d.strftime("%Y%m%d")
                    if ds not in covered:
                        no_data.add(ds)
                    d += timedelta(days=1)

            cursor = c1 + timedelta(days=1)
            save_no_data(no_data)
            if fetched % 50 < 20 and fetched > 0:      # 避免日志刷屏，粗略打进度
                logging.info("进度：已新增 %d 个交易日", fetched)
            time.sleep(BACKFILL_DELAY)
    finally:
        save_no_data(no_data)

    logging.info("回补完成：新增 %d 个交易日，跳过 %d 天，失败 %d 段",
                 fetched, skipped, failed)


def collect_history(limit: int | None = None) -> list[tuple[str, pd.DataFrame]]:
    """读取本地所有 parquet，按日期升序返回。limit=None 表示全部。"""
    files = sorted(DATA_DIR.glob("lhb_*.parquet"))
    if limit is not None and len(files) > limit:
        logging.info("本地共 %d 天数据，只取最近 %d 天", len(files), limit)
        files = files[-limit:]

    history = []
    for f in files:
        try:
            history.append((f.stem.removeprefix("lhb_"), pd.read_parquet(f)))
        except Exception as exc:                      # noqa: BLE001
            logging.warning("读取 %s 失败，跳过：%s", f.name, exc)
    return history


# ------------------------------------------------------------------ 展示层 --

def _fmt_date(date_str: str) -> str:
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"


def _fmt_val(col: str, v) -> str:
    """数值列的展示格式化：涨跌幅/占比/上榜后带 %，金额折成万/亿。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if col in ("涨跌幅", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日",
               "净买额占总成交比"):
        return f"{v:+.2f}%"
    if col in ("换手率", "成交额占总成交比"):
        return f"{v:.2f}%"
    if col == "龙虎榜净买额":
        return f"{v / 1e4:,.0f}万"
    if col in ("流通市值", "市场总成交额"):
        return f"{v / 1e8:,.2f}亿"
    if col == "收盘价":
        return f"{v:.2f}"
    return str(v)


def summarize(df: pd.DataFrame) -> tuple[str | None, pd.DataFrame]:
    """返回 (金额字段名, 按金额降序且按股票代码去重后的表)。"""
    amount_col = next((c for c in AMOUNT_COL_CANDIDATES if c in df.columns), None)
    view = df.sort_values(amount_col, ascending=False) if amount_col else df
    # 同一只股票可能因命中多个上榜原因而出现多行，展示时按代码去重避免刷屏
    if "股票代码" in view.columns:
        view = view.drop_duplicates(subset="股票代码")
    return amount_col, view


def build_body_html(df: pd.DataFrame, date_str: str, page_url: str = "",
                    top: int = 10, health: str = "") -> str:
    """邮件正文：静态 HTML 表格。全部用内联样式，兼容各家邮箱客户端。"""
    amount_col, view = summarize(df)
    stocks = df["股票代码"].nunique() if "股票代码" in df.columns else len(df)

    td = "padding:7px 10px;border-bottom:1px solid #eaeef2;font-size:13px;"
    th = ("padding:8px 10px;border-bottom:2px solid #d0d7de;font-size:12px;"
          "text-align:left;color:#656d76;font-weight:600;white-space:nowrap;")
    num = td + "text-align:right;font-variant-numeric:tabular-nums;"

    # 上榜后N日对「昨天」这类最新日期整列是 NaN，邮件里整列空白没意义，动态剔除
    cols = [c for c in DISPLAY_COLS
            if c in view.columns and view[c].notna().any()]

    rows = []
    for i, (_, r) in enumerate(view.head(top).iterrows()):
        bg = "" if i % 2 == 0 else "background:#f6f8fa;"
        cells = []
        for c in cols:
            v = r[c]
            if c == "涨跌幅":
                color = "#d73a49" if (pd.notna(v) and v >= 0) else "#1a7f37"
                cells.append(f'<td style="{num}{bg}color:{color};'
                             f'font-weight:600;">{_fmt_val(c, v)}</td>')
            elif c in ("收盘价", "龙虎榜净买额", "换手率"):
                cells.append(f'<td style="{num}{bg}">{_fmt_val(c, v)}</td>')
            else:
                cells.append(f'<td style="{td}{bg}">{_fmt_val(c, v)}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")

    head = "".join(f'<th style="{th}">{c}</th>' for c in cols)
    more = (f'<p style="color:#656d76;font-size:12px;margin:12px 0 0;">'
            f'仅显示前 {top} 只，完整 {stocks} 只见在线看板。</p>'
            if stocks > top else "")

    health_line = ""
    if health:
        ok = not health.startswith("⚠")
        color = "#1a7f37" if ok else "#9a6700"
        health_line = (f'<p style="margin:0 0 18px;padding:10px 12px;'
                       f'background:{("#f0fff4" if ok else "#fff8c5")};'
                       f'border:1px solid {color};border-radius:6px;'
                       f'color:{color};font-size:13px;">{health}</p>')

    link = (f'<p style="margin:0 0 20px;"><a href="{page_url}" '
            f'style="display:inline-block;padding:9px 18px;background:#1f6feb;'
            f'color:#ffffff;text-decoration:none;border-radius:6px;'
            f'font-size:14px;font-weight:600;">查看完整看板 · 可切换历史日期 →</a></p>'
            if page_url else "")

    return f"""<!DOCTYPE html><html><body style="margin:0;padding:20px;
background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',
'Microsoft YaHei',sans-serif;color:#1f2328;">
<div style="max-width:760px;margin:0 auto;">
  <h2 style="margin:0 0 4px;font-size:19px;">A股龙虎榜 · {_fmt_date(date_str)}</h2>
  <p style="margin:0 0 18px;color:#656d76;font-size:13px;">
    上榜记录 <b style="color:#1f2328;">{len(df)}</b> 条 ·
    涉及个股 <b style="color:#1f2328;">{stocks}</b> 只
    {f'· 按 {amount_col} 降序' if amount_col else ''}
  </p>
  {health_line}
  {link}
  <table style="border-collapse:collapse;width:100%;">
    <thead><tr>{head}</tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table>
  {more}
  <p style="color:#8b949e;font-size:12px;margin:24px 0 0;
     border-top:1px solid #eaeef2;padding-top:12px;">
    数据来源：东方财富（akshare） · 本邮件由 GitHub Actions 自动发送
  </p>
</div></body></html>"""


def build_body_text(df: pd.DataFrame, date_str: str, page_url: str = "",
                    top: int = 5, health: str = "") -> str:
    """纯文本兜底，给不渲染 HTML 的客户端看。"""
    amount_col, view = summarize(df)
    stocks = df["股票代码"].nunique() if "股票代码" in df.columns else len(df)
    lines = [
        f"A股龙虎榜 · {_fmt_date(date_str)}",
        "",
        f"上榜记录：{len(df)} 条",
        f"涉及个股：{stocks} 只",
    ]
    if health:
        lines += ["", health]
    lines += ["", f"前{top}条（按 {amount_col or '原始顺序'}）："]
    for i, (_, r) in enumerate(view.head(top).iterrows(), start=1):
        code = f" ({r['股票代码']})" if "股票代码" in view.columns else ""
        chg = _fmt_val("涨跌幅", r.get("涨跌幅"))
        net = _fmt_val("龙虎榜净买额", r.get("龙虎榜净买额"))
        name = r.get("股票名称", "")
        lines.append(f"  {i}. {name}{code}  涨跌幅 {chg}  净买额 {net}")
    lines += ["", f"完整看板（可切换历史日期）：{page_url}" if page_url
              else "完整数据见附件 parquet。"]
    return "\n".join(lines)


def write_site(history: list[tuple[str, pd.DataFrame]], target: str,
               health: str = "") -> Path:
    """生成自包含的 output/index.html：所有数据内嵌，页面零 fetch。

    采用列式编码（每列一个数组）+ 股票名称/上榜原因文案全局去重，把体积压到
    行式 JSON 的约 1/3。浏览器只加载 index.html 一个文件，彻底绕开
    「子资源 fetch 被代理/安全软件拦截」这类问题。
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    names: list[str] = []          # 股票名称去重表
    name_index: dict[str, int] = {}
    reasons: list[str] = []        # 上榜原因去重表
    reason_index: dict[str, int] = {}

    def intern_name(n: str) -> int:
        if n not in name_index:
            name_index[n] = len(names)
            names.append(n)
        return name_index[n]

    def col(view: pd.DataFrame, c: str) -> list:
        if c not in view.columns:
            return [None] * len(view)
        # 注意：不能用 .where(pd.notna(...), None)——float64 列里 None 会被
        # 转回 NaN，json 序列化成非法 NaN 而非 null，前端 v == null 拦不住。
        return [None if pd.isna(x) else x for x in view[c].tolist()]

    dates: list[str] = []
    by_date: dict[str, dict] = {}

    for date_str, df in history:
        _, view = summarize(df)

        code = view["股票代码"].astype(str).tolist() if "股票代码" in view.columns else []
        name = [intern_name(str(v)) for v in view["股票名称"]] if "股票名称" in view.columns else []

        inds: list[int | None] = []
        if "上榜原因" in view.columns:
            for v in view["上榜原因"]:
                s = str(v)
                if s not in reason_index:
                    reason_index[s] = len(reasons)
                    reasons.append(s)
                inds.append(reason_index[s])
        else:
            inds = [None] * len(view)

        by_date[date_str] = {
            "total": int(len(df)),
            "stocks": int(df["股票代码"].nunique())
                      if "股票代码" in df.columns else int(len(df)),
            "c": code,            # 股票代码
            "n": name,            # 股票名称 -> names 下标
            "close": col(view, "收盘价"),
            "chg": col(view, "涨跌幅"),
            "net": col(view, "龙虎榜净买额"),
            "turn": col(view, "换手率"),
            "netr": col(view, "净买额占总成交比"),
            "dealr": col(view, "成交额占总成交比"),
            "cap": col(view, "流通市值"),
            "amt": col(view, "市场总成交额"),
            "d1": col(view, "上榜后1日"),
            "d2": col(view, "上榜后2日"),
            "d5": col(view, "上榜后5日"),
            "d10": col(view, "上榜后10日"),
            "r": inds,            # 上榜原因 -> reasons 下标
        }
        dates.append(date_str)

    dates.sort(reverse=True)
    payload = {
        "dates": dates,
        "names": names,
        "reasons": reasons,
        "byDate": by_date,
        "health": health,
        "generated": datetime.now(CST).strftime("%Y-%m-%d %H:%M"),
    }
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")

    index = OUTPUT_DIR / "index.html"
    index.write_text(
        SHELL_HTML.replace("__DATA__", data_json).replace("__TARGET__", target),
        encoding="utf-8",
    )
    return index


# 自包含页面外壳。用普通字符串 + 占位符替换，不用 f-string——JS 里全是花括号，
# f-string 需要把每一个都写成 {{ }}，可读性会毁掉。
SHELL_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>A股龙虎榜</title>
<style>
  :root {
    --bg:#fff; --fg:#1f2328; --muted:#656d76; --border:#d0d7de;
    --zebra:#f6f8fa; --accent:#1f6feb; --field:#fff; --warn:#9a6700;
    --up:#d73a49; --down:#1a7f37;
  }
  @media (prefers-color-scheme:dark) {
    :root {
      --bg:#0d1117; --fg:#e6edf3; --muted:#8b949e; --border:#30363d;
      --zebra:#161b22; --accent:#4493f8; --field:#010409; --warn:#d29922;
      --up:#f85149; --down:#3fb950;
    }
  }
  *{box-sizing:border-box}
  body{margin:0;padding:28px 16px;background:var(--bg);color:var(--fg);
    font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif}
  main{max-width:1500px;margin:0 auto}
  h1{font-size:20px;margin:0 0 6px}
  .range{color:var(--muted);font-size:13px;margin:0 0 18px}
  .bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
  select,input,button{padding:7px 10px;font:inherit;font-size:14px;color:var(--fg);
    background:var(--field);border:1px solid var(--border);border-radius:6px}
  #q{flex:1;min-width:150px}
  button{cursor:pointer}
  button:hover:not(:disabled){border-color:var(--accent);color:var(--accent)}
  button:disabled{opacity:.4;cursor:not-allowed}
  select:focus,input:focus{outline:2px solid var(--accent);outline-offset:-1px}
  .stat{color:var(--muted);font-size:13px;margin-bottom:14px}
  .stat b{color:var(--fg)}
  .health{font-size:13px;padding:8px 12px;border-radius:6px;margin-bottom:14px}
  .health.ok{color:var(--down);background:color-mix(in srgb,var(--down) 10%,var(--bg));border:1px solid var(--down)}
  .health.bad{color:var(--warn);background:color-mix(in srgb,var(--warn) 12%,var(--bg));border:1px solid var(--warn)}
  table{border-collapse:collapse;width:100%;font-size:13px}
  #wrap{overflow-x:auto}
  th:last-child,td:last-child{min-width:260px;white-space:normal}
  th{text-align:center;padding:9px 10px;border-bottom:2px solid var(--border);
    color:var(--muted);font-size:12px;white-space:nowrap;cursor:pointer;user-select:none}
  th:hover{color:var(--accent)}
  th.num,td.num{text-align:center;font-variant-numeric:tabular-nums;white-space:nowrap}
  td.num.empty{color:var(--muted)}
  td{padding:8px 10px;border-bottom:1px solid var(--border)}
  tbody tr:nth-child(even){background:var(--zebra)}
  .up{color:var(--up);font-weight:600}
  .down{color:var(--down);font-weight:600}
  .msg{padding:36px 0;text-align:center;color:var(--muted)}
  .msg.warn{color:var(--warn)}
  footer{margin-top:28px;padding-top:14px;border-top:1px solid var(--border);
    color:var(--muted);font-size:12px}
</style>
</head>
<body>
<main>
  <h1>A股龙虎榜</h1>
  <p class="range" id="range">加载中…</p>
  <div class="bar">
    <button id="prev" title="上一个交易日">←</button>
    <input type="date" id="picker">
    <button id="next" title="下一个交易日">→</button>
    <select id="quick"></select>
    <input id="q" type="text" placeholder="筛选股票名称 / 代码 / 上榜原因…">
  </div>
  <div class="stat" id="stat"></div>
  <div id="wrap"><div class="msg">加载中…</div></div>
  <footer id="foot"></footer>
</main>
<script>
const DATA = __DATA__;
const COLS = ["股票代码","股票名称","收盘价","涨跌幅","龙虎榜净买额","换手率","净买额占总成交比","成交额占总成交比","流通市值","市场总成交额","上榜后1日","上榜后2日","上榜后5日","上榜后10日","上榜原因"];
const NUM  = new Set(["收盘价","涨跌幅","龙虎榜净买额","换手率","净买额占总成交比","成交额占总成交比","流通市值","市场总成交额","上榜后1日","上榜后2日","上榜后5日","上榜后10日"]);
const SIGNED = new Set(["涨跌幅","净买额占总成交比","上榜后1日","上榜后2日","上榜后5日","上榜后10日"]);
const $ = id => document.getElementById(id);

let DATES = DATA.dates;
let cur = "__TARGET__";
let sortCol = null, sortAsc = false;

const compact = s => s.replace(/-/g, "");
const dashed  = s => s.slice(0,4) + "-" + s.slice(4,6) + "-" + s.slice(6);

// 列式数据按行拼回对象（每天最多百余行，开销可忽略）
function rowsOf(d) {
  const p = DATA.byDate[d];
  if (!p) return [];
  const out = new Array(p.c.length);
  for (let k = 0; k < p.c.length; k++) {
    out[k] = {
      code:  p.c[k],
      name:  p.n[k] != null ? DATA.names[p.n[k]] : "",
      close: p.close[k], chg: p.chg[k], net: p.net[k], turn: p.turn[k],
      netr: p.netr[k], dealr: p.dealr[k], cap: p.cap[k], amt: p.amt[k],
      d1: p.d1[k], d2: p.d2[k], d5: p.d5[k], d10: p.d10[k],
      reason: p.r[k] != null ? DATA.reasons[p.r[k]] : "",
    };
  }
  return out;
}
const val = (r, c) =>
  c === "股票代码" ? r.code : c === "股票名称" ? r.name :
  c === "收盘价" ? r.close : c === "涨跌幅" ? r.chg :
  c === "龙虎榜净买额" ? r.net : c === "换手率" ? r.turn :
  c === "净买额占总成交比" ? r.netr : c === "成交额占总成交比" ? r.dealr :
  c === "流通市值" ? r.cap : c === "市场总成交额" ? r.amt :
  c === "上榜后1日" ? r.d1 : c === "上榜后2日" ? r.d2 :
  c === "上榜后5日" ? r.d5 : c === "上榜后10日" ? r.d10 :
  r.reason;

function fmt(r, c) {
  const v = val(r, c);
  if (v == null) return "";
  if (SIGNED.has(c)) return {t: (v > 0 ? "+" : "") + Number(v).toFixed(2) + "%", cls: v >= 0 ? "up" : "down"};
  if (c === "换手率" || c === "成交额占总成交比") return Number(v).toFixed(2) + "%";
  if (c === "龙虎榜净买额") return (Number(v) / 1e4).toLocaleString("zh-CN", {maximumFractionDigits: 0}) + "万";
  if (c === "流通市值" || c === "市场总成交额") return (Number(v) / 1e8).toLocaleString("zh-CN", {minimumFractionDigits: 2, maximumFractionDigits: 2}) + "亿";
  if (c === "收盘价") return Number(v).toFixed(2);
  return v;
}

function boot() {
  if (!DATES.length) { $("wrap").innerHTML = '<div class="msg">暂无归档数据</div>'; return; }
  $("picker").min = dashed(DATES[DATES.length - 1]);
  $("picker").max = dashed(DATES[0]);
  $("quick").innerHTML = DATES.map(d => `<option value="${d}">${dashed(d)}</option>`).join("");
  $("range").textContent =
    `已归档 ${DATES.length} 个交易日：${dashed(DATES[DATES.length-1])} ~ ${dashed(DATES[0])}`;
  const h = DATA.health || "";
  if (h) {
    const el = document.createElement("div");
    el.className = "health " + (h.startsWith("⚠") ? "bad" : "ok");
    el.textContent = h;
    $("wrap").before(el);
  }
  $("foot").textContent =
    `数据来源：东方财富（akshare） · 生成于 ${DATA.generated} (UTC+8) · 单文件版`;
  if (!DATES.includes(cur)) cur = DATES[0];
  show(cur);
}

function nearest(d) {
  if (DATES.includes(d)) return d;
  const earlier = DATES.find(x => x < d);
  return earlier || DATES[DATES.length - 1];
}

function show(d) {
  cur = d;
  $("picker").value = dashed(d);
  $("quick").value = d;
  const i = DATES.indexOf(d);
  $("prev").disabled = i >= DATES.length - 1;
  $("next").disabled = i <= 0;
  render();
}

function render() {
  const p = DATA.byDate[cur];
  if (!p) return;
  const kw = $("q").value.trim().toLowerCase();
  let rows = rowsOf(cur);
  if (kw) rows = rows.filter(r =>
    COLS.some(c => String(val(r, c) ?? "").toLowerCase().includes(kw)));

  if (sortCol) {
    rows = [...rows].sort((a, b) => {
      const x = val(a, sortCol), y = val(b, sortCol);
      const v = NUM.has(sortCol) ? (x ?? -Infinity) - (y ?? -Infinity)
                                 : String(x ?? "").localeCompare(String(y ?? ""), "zh");
      return sortAsc ? v : -v;
    });
  }

  $("stat").innerHTML = `${dashed(cur)} · 上榜记录 <b>${p.total}</b> 条 · ` +
    `涉及个股 <b>${p.stocks}</b> 只` + (kw ? ` · 筛选出 <b>${rows.length}</b> 行` : "");

  if (!rows.length) { $("wrap").innerHTML = '<div class="msg">没有匹配的记录</div>'; return; }

  const head = COLS.map(c => {
    const mark = sortCol === c ? (sortAsc ? " ↑" : " ↓") : "";
    return `<th class="${NUM.has(c) ? "num" : ""}" data-c="${c}">${c}${mark}</th>`;
  }).join("");

  const body = rows.map(r => "<tr>" + COLS.map(c => {
    const v = val(r, c);
    if (NUM.has(c)) {
      if (v == null) return '<td class="num empty">—</td>';
      const f = fmt(r, c);
      if (SIGNED.has(c)) return `<td class="num ${f.cls}">${f.t}</td>`;
      return `<td class="num">${f}</td>`;
    }
    return `<td>${v ?? ""}</td>`;
  }).join("") + "</tr>").join("");

  $("wrap").innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  $("wrap").querySelectorAll("th").forEach(th => th.onclick = () => {
    const c = th.dataset.c;
    if (sortCol === c) sortAsc = !sortAsc; else { sortCol = c; sortAsc = false; }
    render();
  });
}

$("picker").onchange = () => {
  const want = compact($("picker").value);
  if (!want) return;
  const got = nearest(want);
  show(got);
  if (got !== want) {
    $("stat").innerHTML += ` · <span style="color:var(--warn)">` +
      `${dashed(want)} 非交易日或未归档，已跳到 ${dashed(got)}</span>`;
  }
};
$("quick").onchange = () => show($("quick").value);
$("prev").onclick = () => show(DATES[Math.min(DATES.indexOf(cur) + 1, DATES.length - 1)]);
$("next").onclick = () => show(DATES[Math.max(DATES.indexOf(cur) - 1, 0)]);
$("q").oninput = render;

boot();
</script>
</body>
</html>
"""

# ------------------------------------------------------------------ 自检 --

def self_check(history: list[tuple[str, pd.DataFrame]]) -> str:
    """对已归档数据做一次健康自检，返回一行结果文案。

    返回 "" 表示健康；否则返回 "⚠ 自检异常：…" 形式的一句话。
    检查项：字段合法性（必列缺失 / 涨跌幅越界 / 代码缺失）、
    最新日期新鲜度、归档范围内是否有工作日缺口。
    """
    issues: list[str] = []
    if not history:
        return "⚠ 自检异常：无任何归档数据"

    for ds, df in history:
        if df.empty:
            issues.append(f"{ds} 空文件")
            continue
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            issues.append(f"{ds} 缺列 {missing}")
        if "涨跌幅" in df.columns:
            chg = pd.to_numeric(df["涨跌幅"], errors="coerce")
            if chg.isna().all():
                issues.append(f"{ds} 涨跌幅全空")
        if "股票代码" in df.columns and df["股票代码"].isna().any():
            issues.append(f"{ds} 有股票代码缺失")

    dates = sorted(ds for ds, _ in history)
    latest = dates[-1]
    days_ago = (datetime.now(CST).date()
                - datetime.strptime(latest, "%Y%m%d").date()).days
    if days_ago > 7:
        issues.append(f"最新交易日 {latest} 距今 {days_ago} 天，可能已停更")

    # 归档范围内的工作日缺口（非交易日已在台账里，不算缺口）
    if len(dates) >= 2:
        have = set(dates)
        no_data = load_no_data()
        d = datetime.strptime(dates[0], "%Y%m%d").date()
        d1 = datetime.strptime(dates[-1], "%Y%m%d").date()
        while d <= d1:
            ds = d.strftime("%Y%m%d")
            if d.weekday() < 5 and ds not in have and ds not in no_data:
                issues.append(f"{ds} 工作日缺数据")
                if len(issues) > 20:          # 缺口太多就截断，避免文案爆炸
                    break
            d += timedelta(days=1)

    if issues:
        return "⚠ 自检异常：" + "；".join(issues[:10])
    return ""


def _health_text(health: str) -> str:
    return health if health else "✅ 自检通过"


# ---------------------------------------------------------------- 邮件推送 --

def send_email(subject: str, text_body: str, html_body: str,
               attachments: list[tuple[str, bytes, str]]) -> bool:
    """发送 HTML 邮件（带纯文本兜底）。失败只记录错误，不抛异常。

    attachments: [(文件名, 内容, subtype), ...]
    """
    sender = os.environ.get("EMAIL_SENDER")
    password = os.environ.get("EMAIL_PASSWORD")
    receiver = os.environ.get("EMAIL_RECEIVER")
    server = os.environ.get("SMTP_SERVER")
    port = os.environ.get("SMTP_PORT", "465")

    missing = [k for k, v in {
        "EMAIL_SENDER": sender, "EMAIL_PASSWORD": password,
        "EMAIL_RECEIVER": receiver, "SMTP_SERVER": server,
    }.items() if not v]
    if missing:
        logging.error("缺少环境变量 %s，跳过邮件发送", ", ".join(missing))
        return False

    # 支持多个收件人，中英文逗号/分号分隔
    recipients = [r.strip() for r in receiver.replace("；", ";").replace("，", ",")
                  .replace(";", ",").split(",") if r.strip()]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    for name, blob, subtype in attachments:
        msg.add_attachment(blob, maintype="application", subtype=subtype, filename=name)

    try:
        port_num = int(port)
    except ValueError:
        logging.error("SMTP_PORT 不是合法端口号：%r，跳过邮件发送", port)
        return False

    try:
        logging.info("正在通过 %s:%d 发送邮件给 %s…", server, port_num, recipients)
        if port_num == 465:
            smtp = smtplib.SMTP_SSL(server, port_num, timeout=60)
        else:
            smtp = smtplib.SMTP(server, port_num, timeout=60)
        with smtp:
            if port_num != 465:
                smtp.starttls()
            smtp.login(sender, password)
            smtp.send_message(msg)
        logging.info("邮件发送成功（附件 %d 个）", len(attachments))
        return True
    except Exception as exc:                          # noqa: BLE001 - 不因发信失败中断
        logging.error("邮件发送失败: %s: %s", type(exc).__name__, exc)
        return False


# ------------------------------------------------------------------ 主流程 --

def main() -> int:
    setup_logging()
    args = parse_args()
    page_url = (args.page_url or os.environ.get("PAGE_URL", "")).strip()

    if args.backfill > 0 and not args.email_only:
        backfill(args.backfill)

    try:
        date_str = resolve_date(args.date)
    except ValueError:
        logging.error("--date 格式应为 YYYYMMDD 或 YYYY-MM-DD，收到：%r", args.date)
        return EXIT_FETCH_FAILED

    logging.info("=== 龙虎榜任务开始，目标日期 %s ===", date_str)

    # --email-only 只负责发信，本地没有当日数据就直接收工，不要再去请求一次
    if args.email_only and not parquet_path(date_str).exists():
        logging.info("%s 无本地数据（非交易日或前序步骤未抓到），无可发送内容", date_str)
        return EXIT_OK

    df = load_or_fetch(date_str)
    if df is None:
        return EXIT_FETCH_FAILED

    no_data = df.empty
    if no_data:
        logging.info("%s 无龙虎榜数据（非交易日或当日无个股上榜）", date_str)

    # 看板无论当日有没有数据都要重新生成——否则非交易日那天 Pages 没产物可传。
    # 顺带对全量历史做一次健康自检，结果写进看板和邮件。
    health = ""
    if args.email_only:
        # 只发信也要带上自检结果（看板已在上一步生成并部署，不再重复生成）
        health = self_check(collect_history())
    else:
        history = collect_history()
        health = self_check(history)
        index = write_site(history, date_str, health)
        logging.info("已生成 %s：%d 个交易日、%.1f KB（gzip 后约 1/2.4）",
                     index.relative_to(ROOT), len(history),
                     index.stat().st_size / 1024)
    logging.info("自检：%s", _health_text(health))

    if no_data:
        logging.info("当日无数据，不发邮件")
        return EXIT_OK

    if args.no_email:
        logging.info("--no-email 已指定，跳过邮件发送")
        return EXIT_OK

    if not page_url:
        logging.warning("未提供 --page-url / PAGE_URL，邮件正文不含看板链接")

    stocks = df["股票代码"].nunique() if "股票代码" in df.columns else len(df)
    health_tag = "自检异常" if health.startswith("⚠") else ""
    subject = f"【龙虎榜{('·' + health_tag) if health_tag else ''}】" \
              f"{_fmt_date(date_str)} 上榜 {stocks} 只"
    send_email(
        subject=subject,
        text_body=build_body_text(df, date_str, page_url, health=_health_text(health)),
        html_body=build_body_html(df, date_str, page_url, health=_health_text(health)),
        attachments=[
            (f"lhb_{date_str}.parquet", parquet_path(date_str).read_bytes(), "octet-stream"),
        ],
    )
    logging.info("=== 任务结束 ===")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
