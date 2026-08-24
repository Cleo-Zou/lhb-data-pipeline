# A股龙虎榜每日自动化获取与邮件推送

用 Python + GitHub Actions 每个交易日自动抓取龙虎榜数据，存为 Parquet 并邮件推送。
**完全免费，无需任何 Token**（数据源为 akshare 抓取的新浪财经公开页面）。

## 工作流程

```
GitHub Actions (工作日北京时间 16:00)
  └─ fetch_lhb.py
       ├─ akshare 抓取昨天的龙虎榜明细（失败重试 3 次，间隔 5 秒）
       ├─ 清洗 → 保存 data/lhb_YYYYMMDD.parquet
       └─ 发送邮件（正文含日期 / 总条数 / 前 5 条摘要，附件为 Parquet）
```

## 文件说明

| 文件 | 作用 |
| --- | --- |
| `fetch_lhb.py` | 主脚本：抓取、清洗、存储、发信 |
| `.github/workflows/lhb_daily.yml` | 定时任务配置 |
| `requirements.txt` | 依赖：akshare / pandas / pyarrow |

## 部署步骤

### 1. 推送代码到 GitHub 仓库

```bash
git init && git add . && git commit -m "init"
git remote add origin <你的仓库地址>
git push -u origin main
```

### 2. 配置 Secrets（**关键步骤**）

进入仓库 → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**，
依次添加以下 5 个：

| Secret 名称 | 说明 | 示例 |
| --- | --- | --- |
| `EMAIL_SENDER` | 发件邮箱地址 | `yourname@qq.com` |
| `EMAIL_PASSWORD` | 邮箱**授权码**（不是登录密码！） | `abcdefghijklmnop` |
| `EMAIL_RECEIVER` | 收件邮箱，多个用英文逗号分隔 | `a@qq.com,b@163.com` |
| `SMTP_SERVER` | SMTP 服务器地址 | `smtp.qq.com` |
| `SMTP_PORT` | 端口：`465`(SSL) 或 `587`(TLS) | `465` |

**授权码怎么获取**（以 QQ 邮箱为例）：
邮箱设置 → 账号 → 开启「POP3/SMTP 服务」→ 按提示发送短信 → 获得 16 位授权码。
163 邮箱同理，在「设置 → POP3/SMTP/IMAP」中开启并获取客户端授权密码。

常见邮箱 SMTP 配置：

| 邮箱 | SMTP_SERVER | SMTP_PORT |
| --- | --- | --- |
| QQ | `smtp.qq.com` | `465` |
| 163 | `smtp.163.com` | `465` |
| Gmail | `smtp.gmail.com` | `587` |
| Outlook | `smtp.office365.com` | `587` |

### 3. 启用 Actions

仓库 → **Actions** 标签页 → 若提示则点击启用。之后可在 **LHB Daily** 工作流中点
**Run workflow** 手动触发一次，验证配置是否正确（支持填入指定日期）。

## 本地运行

```bash
pip install -r requirements.txt

python fetch_lhb.py --date 20260821 --no-email   # 只抓取存盘，不发信
python fetch_lhb.py --date 20260821              # 需先设置好环境变量
```

Windows PowerShell 下设置环境变量：

```powershell
$env:EMAIL_SENDER="yourname@qq.com"; $env:EMAIL_PASSWORD="授权码"
$env:EMAIL_RECEIVER="a@qq.com"; $env:SMTP_SERVER="smtp.qq.com"; $env:SMTP_PORT="465"
```

## 注意事项

- **默认取「昨天」的数据**，因为交易日数据次日才完整。因此周一运行时取到的是周日
  （非交易日），脚本会记录日志并正常退出，不发邮件——这是预期行为。若想改为抓取当天，
  把 `resolve_date()` 中的 `timedelta(days=1)` 改成 `timedelta(days=0)` 即可。
- **数据字段**：`stock_lhb_detail_daily_sina` 返回的是新浪「每日详情」榜，字段为
  `序号 / 股票代码 / 股票名称 / 收盘价 / 对应值 / 成交量 / 成交额 / 指标`，
  **不含「机构净额」**。摘要按 `成交额` 降序展示前 5 条；若后续换用含 `机构净额`
  的接口（如 `ak.stock_lhb_jgmmtj_em`），脚本会自动优先使用该字段，无需改代码。
- 非交易日、接口无数据时跳过保存与发信，退出码 0（Actions 不会标红）。
- 邮件发送失败只打印错误、不中断脚本；Parquet 文件仍会作为 Actions artifact
  保留 30 天，可从运行记录页面下载。
- GitHub Actions 的定时任务在高峰期可能延迟数分钟至数十分钟触发，属正常现象。
- 免费额度：public 仓库无限制；private 仓库每月 2000 分钟，本任务每次约 1-2 分钟，
  完全够用。
