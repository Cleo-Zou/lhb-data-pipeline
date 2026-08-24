#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股龙虎榜每日数据获取与邮件推送。

数据源: akshare `stock_lhb_detail_daily_sina`（新浪财经，免费、无需 Token）

用法:
    python fetch_lhb.py                 # 抓取昨天（北京时间）的数据
    python fetch_lhb.py --date 20260821 # 抓取指定日期的数据
"""

from __future__ import annotations

import argparse
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
DATA_DIR = Path(__file__).resolve().parent / "data"
CST = timezone(timedelta(hours=8))   # 北京时间，中国无夏令时，固定偏移即可

# 摘要中优先展示的金额字段（新浪日榜无「机构净额」，退化为「成交额」）
AMOUNT_COL_CANDIDATES = ("机构净额", "成交额", "对应值")

# 退出码
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
        "--date",
        default=None,
        metavar="YYYYMMDD",
        help="指定交易日期，默认为北京时间的昨天",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="只抓取并保存，不发送邮件（本地调试用）",
    )
    return parser.parse_args()


def resolve_date(explicit: str | None) -> str:
    """确定目标日期。默认取昨天——交易日数据次日才完整。"""
    if explicit:
        # 提前校验格式，避免带着脏参数去调接口
        datetime.strptime(explicit, "%Y%m%d")
        return explicit
    return (datetime.now(CST) - timedelta(days=1)).strftime("%Y%m%d")


# ---------------------------------------------------------------- 数据获取 --

def fetch_lhb(date_str: str) -> pd.DataFrame | None:
    """抓取指定日期的龙虎榜明细，失败重试 MAX_RETRIES 次。

    返回 DataFrame；接口正常但当日无数据（非交易日）时返回空 DataFrame；
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


def save_parquet(df: pd.DataFrame, date_str: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"lhb_{date_str}.parquet"
    df.to_parquet(path, index=False)
    logging.info("已保存 %d 条数据到 %s（%.1f KB）",
                 len(df), path, path.stat().st_size / 1024)
    return path


# ---------------------------------------------------------------- 邮件推送 --

def build_body(df: pd.DataFrame, date_str: str, path: Path) -> str:
    """生成邮件正文：日期、总条数、前5条摘要。"""
    lines = [
        f"A股龙虎榜数据日报",
        "",
        f"数据日期：{date_str}",
        f"数据总条数：{len(df)}",
        f"生成时间：{datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)",
        "",
    ]

    name_col = "股票名称" if "股票名称" in df.columns else df.columns[0]
    amount_col = next((c for c in AMOUNT_COL_CANDIDATES if c in df.columns), None)

    head = df.sort_values(amount_col, ascending=False) if amount_col else df
    # 同一只股票可能因命中多个上榜指标而出现多行，摘要里按代码去重避免刷屏
    if "股票代码" in head.columns:
        head = head.drop_duplicates(subset="股票代码")
        lines.append(f"上榜个股数：{head['股票代码'].nunique()}")
        lines.append("")

    lines.append(f"前5条摘要（按 {amount_col or '原始顺序'}）：")
    for i, (_, row) in enumerate(head.head(5).iterrows(), start=1):
        name = row[name_col]
        code = f" ({row['股票代码']})" if "股票代码" in df.columns else ""
        if amount_col:
            lines.append(f"  {i}. {name}{code}  {amount_col}：{row[amount_col]:,.2f}")
        else:
            lines.append(f"  {i}. {name}{code}")

    lines += ["", f"完整数据见附件：{path.name}", "", "-- 本邮件由 GitHub Actions 自动发送"]
    return "\n".join(lines)


def send_email(subject: str, body: str, attachment: Path) -> bool:
    """发送带附件的邮件。失败只记录错误，由调用方决定是否继续。"""
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

    # 支持多个收件人，逗号或分号分隔
    recipients = [r.strip() for r in receiver.replace("；", ";").replace("，", ",")
                  .replace(";", ",").split(",") if r.strip()]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    msg.add_attachment(
        attachment.read_bytes(),
        maintype="application",
        subtype="octet-stream",
        filename=attachment.name,
    )

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
        logging.info("邮件发送成功")
        return True
    except Exception as exc:                          # noqa: BLE001 - 不因发信失败中断
        logging.error("邮件发送失败: %s: %s", type(exc).__name__, exc)
        return False


# -------------------------------------------------------------------- 主流程 --

def main() -> int:
    setup_logging()
    args = parse_args()

    try:
        date_str = resolve_date(args.date)
    except ValueError:
        logging.error("--date 格式应为 YYYYMMDD，收到：%r", args.date)
        return EXIT_FETCH_FAILED

    logging.info("=== 龙虎榜任务开始，目标日期 %s ===", date_str)

    df = fetch_lhb(date_str)
    if df is None:
        return EXIT_FETCH_FAILED

    if df.empty:
        logging.info("%s 无龙虎榜数据（非交易日或当日无个股上榜），跳过保存与发信", date_str)
        return EXIT_OK

    df = clean(df)
    if df.empty:
        logging.info("清洗后无有效数据，跳过保存与发信")
        return EXIT_OK

    path = save_parquet(df, date_str)

    if args.no_email:
        logging.info("--no-email 已指定，跳过邮件发送")
        return EXIT_OK

    send_email(
        subject=f"【龙虎榜】{date_str} 共 {len(df)} 条",
        body=build_body(df, date_str, path),
        attachment=path,
    )
    logging.info("=== 任务结束 ===")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
