#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股龙虎榜每日数据获取与邮件推送。

数据源: akshare `stock_lhb_detail_daily_sina`（新浪财经，免费、无需 Token）

每个交易日的数据首次抓取后落成 data/lhb_YYYYMMDD.parquet，之后复用不再重复请求。
再由全部历史 parquet 生成 output/index.html（可切换日期的看板，部署到 GitHub Pages），
邮件正文放当日榜单表格 + 看板链接。

用法:
    python fetch_lhb.py                    # 抓取昨天（北京时间）的数据
    python fetch_lhb.py --date 2026-08-21  # 抓取指定日期（两种格式都认）
    python fetch_lhb.py --backfill 30      # 回补最近 30 天，攒历史
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

MAX_RETRIES = 3           # akshare 接口最大重试次数
RETRY_DELAY = 5           # 重试间隔（秒）
BACKFILL_DELAY = 1.5      # 回补时每次请求之间的间隔，避免把新浪打急了
MAX_HISTORY_DAYS = 120    # 页面里最多嵌入多少个交易日，约 10 KB/天

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"      # GitHub Pages 部署目录
CST = timezone(timedelta(hours=8))   # 北京时间，中国无夏令时，固定偏移即可

# 摘要中优先展示的金额字段（新浪日榜无「机构净额」，退化为「成交额」）
AMOUNT_COL_CANDIDATES = ("机构净额", "成交额", "对应值")

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


# ---------------------------------------------------------------- 数据获取 --

def fetch_lhb(date_str: str) -> pd.DataFrame | None:
    """抓取指定日期的龙虎榜明细，失败重试 MAX_RETRIES 次。

    返回 DataFrame；当日无数据（非交易日）时返回空 DataFrame；
    重试耗尽仍失败返回 None。
    """
    import akshare as ak   # 延迟导入：import akshare 本身耗时约数秒

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info("正在获取 %s 龙虎榜数据（第 %d/%d 次尝试）…",
                         date_str, attempt, MAX_RETRIES)
            df = ak.stock_lhb_detail_daily_sina(date=date_str)
            if df is None:
                logging.warning("接口返回 None，视为无数据")
                return pd.DataFrame()
            logging.info("接口返回 %d 行、%d 列", len(df), len(df.columns))
            return df
        except KeyError as exc:
            # 非交易日时页面没有数据表，akshare 内部对空 DataFrame 取列会抛
            # KeyError('股票代码')。这是确定性的「无数据」，重试没有意义。
            logging.info("%s 无数据表（非交易日），接口抛 KeyError(%s)", date_str, exc)
            return pd.DataFrame()
        except Exception as exc:                      # noqa: BLE001 - 接口异常类型不确定
            logging.warning("第 %d 次获取失败: %s: %s",
                            attempt, type(exc).__name__, exc)
            if attempt < MAX_RETRIES:
                logging.info("%d 秒后重试…", RETRY_DELAY)
                time.sleep(RETRY_DELAY)

    logging.error("重试 %d 次后仍无法获取 %s 的数据", MAX_RETRIES, date_str)
    return None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """清洗：去全空行、去重、清理字符串空白、填充文本缺失值。"""
    before = len(df)
    df = df.dropna(how="all").copy()

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

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

    df = clean(df)
    if df.empty:
        logging.info("清洗后无有效数据，不保存")
        return df

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logging.info("已保存 %d 条数据到 %s（%.1f KB）",
                 len(df), path.name, path.stat().st_size / 1024)
    return df


def backfill(days: int) -> None:
    """回补最近 N 个自然日；已有 parquet 的直接跳过。"""
    logging.info("=== 开始回补最近 %d 天 ===", days)
    fetched = skipped = empty = 0
    for i in range(1, days + 1):
        date_str = (datetime.now(CST) - timedelta(days=i)).strftime("%Y%m%d")
        if parquet_path(date_str).exists():
            skipped += 1
            continue
        df = load_or_fetch(date_str)
        if df is None:
            logging.warning("%s 抓取失败，跳过", date_str)
        elif df.empty:
            empty += 1
        else:
            fetched += 1
        time.sleep(BACKFILL_DELAY)   # 别把新浪打急了
    logging.info("回补完成：新增 %d 天，已存在跳过 %d 天，非交易日 %d 天",
                 fetched, skipped, empty)


def collect_history(limit: int = MAX_HISTORY_DAYS) -> list[tuple[str, pd.DataFrame]]:
    """读取本地所有 parquet，按日期倒序返回最近 limit 天。"""
    files = sorted(DATA_DIR.glob("lhb_*.parquet"), reverse=True)
    if len(files) > limit:
        logging.info("本地共 %d 天数据，HTML 只嵌入最近 %d 天（更早的仍在 data/ 里）",
                     len(files), limit)
        files = files[:limit]

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


def summarize(df: pd.DataFrame) -> tuple[str | None, pd.DataFrame]:
    """返回 (金额字段名, 按金额降序且按股票代码去重后的表)。"""
    amount_col = next((c for c in AMOUNT_COL_CANDIDATES if c in df.columns), None)
    view = df.sort_values(amount_col, ascending=False) if amount_col else df
    # 同一只股票可能因命中多个上榜指标而出现多行，展示时按代码去重避免刷屏
    if "股票代码" in view.columns:
        view = view.drop_duplicates(subset="股票代码")
    return amount_col, view


def build_body_html(df: pd.DataFrame, date_str: str,
                    page_url: str = "", top: int = 10) -> str:
    """邮件正文：静态 HTML 表格。全部用内联样式，兼容各家邮箱客户端。"""
    amount_col, view = summarize(df)
    stocks = df["股票代码"].nunique() if "股票代码" in df.columns else len(df)

    td = "padding:7px 10px;border-bottom:1px solid #eaeef2;font-size:13px;"
    th = ("padding:8px 10px;border-bottom:2px solid #d0d7de;font-size:12px;"
          "text-align:left;color:#656d76;font-weight:600;white-space:nowrap;")
    num = td + "text-align:right;font-variant-numeric:tabular-nums;"

    cols = [c for c in ("股票代码", "股票名称", "收盘价", "对应值", "成交额", "指标")
            if c in view.columns]

    rows = []
    for i, (_, r) in enumerate(view.head(top).iterrows()):
        bg = "" if i % 2 == 0 else "background:#f6f8fa;"
        cells = []
        for c in cols:
            v = r[c]
            if isinstance(v, (int, float)) and pd.notna(v):
                cells.append(f'<td style="{num}{bg}">{v:,.2f}</td>')
            else:
                cells.append(f'<td style="{td}{bg}">{v}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")

    head = "".join(f'<th style="{th}">{c}</th>' for c in cols)
    more = (f'<p style="color:#656d76;font-size:12px;margin:12px 0 0;">'
            f'仅显示前 {top} 只，完整 {stocks} 只见在线看板。</p>'
            if stocks > top else "")

    # 看板链接放正文顶部，一眼能点到
    link = (f'<p style="margin:0 0 20px;"><a href="{page_url}" '
            f'style="display:inline-block;padding:9px 18px;background:#1f6feb;'
            f'color:#ffffff;text-decoration:none;border-radius:6px;'
            f'font-size:14px;font-weight:600;">查看完整看板 · 可切换历史日期 →</a></p>'
            if page_url else "")

    return f"""<!DOCTYPE html><html><body style="margin:0;padding:20px;
background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',
'Microsoft YaHei',sans-serif;color:#1f2328;">
<div style="max-width:720px;margin:0 auto;">
  <h2 style="margin:0 0 4px;font-size:19px;">A股龙虎榜 · {_fmt_date(date_str)}</h2>
  <p style="margin:0 0 18px;color:#656d76;font-size:13px;">
    上榜记录 <b style="color:#1f2328;">{len(df)}</b> 条 ·
    涉及个股 <b style="color:#1f2328;">{stocks}</b> 只
    {f'· 按 {amount_col} 降序' if amount_col else ''}
  </p>
  {link}
  <table style="border-collapse:collapse;width:100%;">
    <thead><tr>{head}</tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table>
  {more}
  <p style="color:#8b949e;font-size:12px;margin:24px 0 0;
     border-top:1px solid #eaeef2;padding-top:12px;">
    数据来源：新浪财经（akshare） · 本邮件由 GitHub Actions 自动发送
  </p>
</div></body></html>"""


def build_body_text(df: pd.DataFrame, date_str: str,
                    page_url: str = "", top: int = 5) -> str:
    """纯文本兜底，给不渲染 HTML 的客户端看。"""
    amount_col, view = summarize(df)
    lines = [
        f"A股龙虎榜 · {_fmt_date(date_str)}",
        "",
        f"上榜记录：{len(df)} 条",
        f"涉及个股：{df['股票代码'].nunique() if '股票代码' in df.columns else len(df)} 只",
        "",
        f"前{top}条（按 {amount_col or '原始顺序'}）：",
    ]
    for i, (_, r) in enumerate(view.head(top).iterrows(), start=1):
        code = f" ({r['股票代码']})" if "股票代码" in view.columns else ""
        amt = f"  {amount_col}：{r[amount_col]:,.2f}" if amount_col else ""
        lines.append(f"  {i}. {r.get('股票名称', '')}{code}{amt}")
    lines += ["", f"完整看板（可切换历史日期）：{page_url}" if page_url
              else "完整数据见附件 parquet。"]
    return "\n".join(lines)


def build_interactive_html(history: list[tuple[str, pd.DataFrame]],
                           target: str) -> str:
    """生成可切换日期的独立 HTML 页面（附件用，浏览器打开后 JS 生效）。"""
    payload = {}
    indicators: list[str] = []      # 指标文案去重表：几十字的长串，每行重复存太浪费
    ind_index: dict[str, int] = {}

    for date_str, df in history:
        _, view = summarize(df)
        view = view.drop(columns=[c for c in ("序号",) if c in view.columns])
        # NaN 不是合法 JSON，转成 None
        rows = view.where(pd.notna(view), None).to_dict(orient="records")
        for r in rows:
            ind = r.pop("指标", None)
            if ind is not None:
                if ind not in ind_index:
                    ind_index[ind] = len(indicators)
                    indicators.append(ind)
                r["i"] = ind_index[ind]
        payload[date_str] = {
            "rows": rows,
            "total": int(len(df)),
            "stocks": int(df["股票代码"].nunique()) if "股票代码" in df.columns else int(len(df)),
        }

    # ensure_ascii=False 保留中文；闭合标签转义，避免提前截断 <script>
    def dump(obj: object) -> str:
        return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")

    data_json, ind_json = dump(payload), dump(indicators)
    dates_desc = sorted(payload.keys(), reverse=True)
    default = target if target in payload else (dates_desc[0] if dates_desc else "")
    generated = datetime.now(CST).strftime("%Y-%m-%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>A股龙虎榜</title>
<style>
  :root {{
    --bg:#fff; --fg:#1f2328; --muted:#656d76; --border:#d0d7de;
    --zebra:#f6f8fa; --accent:#1f6feb; --field:#fff;
  }}
  @media (prefers-color-scheme:dark) {{
    :root {{
      --bg:#0d1117; --fg:#e6edf3; --muted:#8b949e; --border:#30363d;
      --zebra:#161b22; --accent:#4493f8; --field:#010409;
    }}
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;padding:28px 16px;background:var(--bg);color:var(--fg);
    font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif}}
  main{{max-width:960px;margin:0 auto}}
  h1{{font-size:20px;margin:0 0 20px}}
  .bar{{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}}
  select,input{{padding:7px 10px;font:inherit;font-size:14px;color:var(--fg);
    background:var(--field);border:1px solid var(--border);border-radius:6px}}
  input{{flex:1;min-width:160px}}
  select:focus,input:focus{{outline:2px solid var(--accent);outline-offset:-1px}}
  .stat{{color:var(--muted);font-size:13px;margin-bottom:14px}}
  .stat b{{color:var(--fg)}}
  table{{border-collapse:collapse;width:100%;font-size:13px}}
  th{{text-align:left;padding:9px 10px;border-bottom:2px solid var(--border);
    color:var(--muted);font-size:12px;white-space:nowrap;cursor:pointer;user-select:none}}
  th:hover{{color:var(--accent)}}
  th.num,td.num{{text-align:right;font-variant-numeric:tabular-nums}}
  td{{padding:8px 10px;border-bottom:1px solid var(--border)}}
  tbody tr:nth-child(even){{background:var(--zebra)}}
  .empty{{padding:40px 0;text-align:center;color:var(--muted)}}
  footer{{margin-top:28px;padding-top:14px;border-top:1px solid var(--border);
    color:var(--muted);font-size:12px}}
</style>
</head>
<body>
<main>
  <h1>A股龙虎榜</h1>
  <div class="bar">
    <select id="date"></select>
    <input id="q" type="text" placeholder="筛选股票名称 / 代码 / 指标…">
  </div>
  <div class="stat" id="stat"></div>
  <div id="wrap"></div>
  <footer>
    数据来源：新浪财经（akshare） · 生成于 {generated} (UTC+8) ·
    共 {len(payload)} 个交易日
  </footer>
</main>
<script>
const DATA = {data_json};
const IND = {ind_json};          // 指标文案表，行里只存下标
const COLS = ["股票代码","股票名称","收盘价","对应值","成交量","成交额","指标"];
const NUM = new Set(["收盘价","对应值","成交量","成交额"]);
const val = (r, c) => c === "指标" ? (IND[r.i] ?? "") : r[c];
const $ = id => document.getElementById(id);
let sortCol = null, sortAsc = false;

const dates = Object.keys(DATA).sort().reverse();
$("date").innerHTML = dates.map(d =>
  `<option value="${{d}}">${{d.slice(0,4)}}-${{d.slice(4,6)}}-${{d.slice(6)}}` +
  ` （${{DATA[d].stocks}} 只）</option>`).join("");
$("date").value = "{default}";

function render() {{
  const d = $("date").value, pack = DATA[d];
  if (!pack) {{ $("wrap").innerHTML = '<div class="empty">该日无数据</div>'; return; }}

  const kw = $("q").value.trim().toLowerCase();
  let rows = pack.rows.filter(r => !kw ||
    COLS.some(c => String(val(r, c) ?? "").toLowerCase().includes(kw)));

  if (sortCol) {{
    rows = [...rows].sort((a, b) => {{
      const x = val(a, sortCol), y = val(b, sortCol);
      const v = NUM.has(sortCol) ? (x ?? -Infinity) - (y ?? -Infinity)
                                 : String(x ?? "").localeCompare(String(y ?? ""), "zh");
      return sortAsc ? v : -v;
    }});
  }}

  $("stat").innerHTML = `上榜记录 <b>${{pack.total}}</b> 条 · 涉及个股 ` +
    `<b>${{pack.stocks}}</b> 只` + (kw ? ` · 筛选出 <b>${{rows.length}}</b> 行` : "");

  if (!rows.length) {{ $("wrap").innerHTML = '<div class="empty">没有匹配的记录</div>'; return; }}

  const head = COLS.map(c => {{
    const mark = sortCol === c ? (sortAsc ? " ↑" : " ↓") : "";
    return `<th class="${{NUM.has(c) ? "num" : ""}}" data-c="${{c}}">${{c}}${{mark}}</th>`;
  }}).join("");

  const body = rows.map(r => "<tr>" + COLS.map(c => {{
    const v = val(r, c);
    if (NUM.has(c)) {{
      const t = (v == null) ? "" : Number(v).toLocaleString("zh-CN",
        {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
      return `<td class="num">${{t}}</td>`;
    }}
    return `<td>${{v ?? ""}}</td>`;
  }}).join("") + "</tr>").join("");

  $("wrap").innerHTML = `<table><thead><tr>${{head}}</tr></thead><tbody>${{body}}</tbody></table>`;
  $("wrap").querySelectorAll("th").forEach(th => th.onclick = () => {{
    const c = th.dataset.c;
    if (sortCol === c) sortAsc = !sortAsc; else {{ sortCol = c; sortAsc = false; }}
    render();
  }});
}}

$("date").onchange = render;
$("q").oninput = render;
render();
</script>
</body>
</html>"""


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

    # 看板无论当日有没有数据都要重新生成——否则非交易日那天 Pages 没产物可传
    if not args.email_only:
        history = collect_history()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out = OUTPUT_DIR / "index.html"
        out.write_text(build_interactive_html(history, date_str), encoding="utf-8")
        logging.info("已生成 %s，含 %d 个交易日、%.1f KB",
                     out.relative_to(ROOT), len(history), out.stat().st_size / 1024)

    if no_data:
        logging.info("当日无数据，不发邮件")
        return EXIT_OK

    if args.no_email:
        logging.info("--no-email 已指定，跳过邮件发送")
        return EXIT_OK

    if not page_url:
        logging.warning("未提供 --page-url / PAGE_URL，邮件正文不含看板链接")

    stocks = df["股票代码"].nunique() if "股票代码" in df.columns else len(df)
    send_email(
        subject=f"【龙虎榜】{_fmt_date(date_str)} 上榜 {stocks} 只",
        text_body=build_body_text(df, date_str, page_url),
        html_body=build_body_html(df, date_str, page_url),
        attachments=[
            (f"lhb_{date_str}.parquet", parquet_path(date_str).read_bytes(), "octet-stream"),
        ],
    )
    logging.info("=== 任务结束 ===")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
