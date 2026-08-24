# A股龙虎榜每日自动化获取与邮件推送

用 Python + GitHub Actions 每个交易日自动抓取龙虎榜数据，存为 Parquet 并邮件推送一份
可交互的 HTML 榜单。**完全免费，无需任何 Token**（数据源为 akshare 抓取的新浪财经公开页面）。

## 工作流程

```
GitHub Actions (工作日北京时间 16:00)
  └─ fetch_lhb.py
       ├─ 先查 data/lhb_YYYYMMDD.parquet —— 有就直接用，不重复请求
       ├─ 没有才调 akshare 抓取（失败重试 3 次，间隔 5 秒）→ 清洗 → 落盘
       ├─ 读取本地全部历史 parquet，生成可切换日期的 HTML
       └─ 发邮件：HTML 正文（当日榜单）+ HTML 附件（含历史）+ 当日 parquet
  └─ 把 data/ 提交回仓库，历史就此累积
```

## 邮件里有什么

| 部分 | 内容 |
| --- | --- |
| **正文** | 当日榜单前 10 名的 HTML 表格，纯内联样式，各家邮箱都能正常渲染 |
| **附件 `龙虎榜_YYYYMMDD.html`** | 完整榜单，**可切换历史日期**、按列排序、关键词筛选 |
| **附件 `lhb_YYYYMMDD.parquet`** | 当日原始数据，给 pandas 用 |

> 邮件客户端会剥掉 `<script>`，所以交互功能在正文里用不了。把 HTML 附件下载下来用
> 浏览器打开，日期下拉、排序、筛选就都能用了——附件是自包含的，离线也能看。

## 历史数据怎么累积

GitHub Actions 的 runner 每次都是全新的临时环境，`data/` 不会自己保留。这里用了两层
（参考 `Index_Enhancement_Monitor` 的做法）：

1. **提交回仓库**（主）：每次运行后 `git add data/ && git commit && git push`，
   历史数据随仓库一起版本化。单日 parquet 约 9 KB，一年约 2 MB，完全不占地方。
2. **`actions/cache`**（兜底）：万一某次 push 撞上冲突没提交成功，下次运行仍能从缓存
   恢复。key 用 `github.run_id` 保证每次写新条目，`restore-keys` 前缀匹配拿回最近一份。

脚本侧配合的是 **parquet 优先**：`load_or_fetch()` 发现本地已有该日文件就直接读，
不再请求新浪。所以重复运行、回补历史都不会浪费请求。

### 首次攒历史

新仓库 `data/` 是空的，HTML 里只有一天可选。跑一次回补：

Actions → **LHB Daily** → Run workflow → **backfill** 填 `60` → 运行。
会把最近 60 个自然日里的交易日全部补齐（已存在的跳过），完成后提交回仓库。

本地跑也一样：

```bash
python fetch_lhb.py --backfill 60 --no-email
```

## 部署步骤

### 1. 配置 Secrets（**关键步骤**）

仓库 → **Settings** → **Secrets and variables** → **Actions** → 停在 **Secrets** 标签页
（不是 Variables）→ **New repository secret**，依次添加这 5 个：

| Secret 名称 | 说明 | 示例 |
| --- | --- | --- |
| `EMAIL_SENDER` | 发件邮箱地址 | `yourname@163.com` |
| `EMAIL_PASSWORD` | 邮箱**授权码**（不是登录密码！） | `ABCDEFGHIJKLMNOP` |
| `EMAIL_RECEIVER` | 收件邮箱，多个用英文逗号分隔 | `a@163.com,b@qq.com` |
| `SMTP_SERVER` | SMTP 服务器地址 | `smtp.163.com` |
| `SMTP_PORT` | 端口：`465`(SSL) 或 `587`(TLS) | `465` |

要用 **Repository secrets**，不是 Environment secrets——后者要求 job 里声明
`environment:`，本 workflow 没有，会读不到值（表现为「缺少环境变量，跳过邮件发送」）。

**授权码怎么获取**：163 邮箱 → 设置 → POP3/SMTP/IMAP → 开启 SMTP 服务 → 扫码验证
→ 生成「客户端授权密码」。QQ 邮箱在「设置 → 账号 → POP3/SMTP 服务」，发短信后得到
16 位授权码。授权码是邮箱级凭证，一串可以同时给多个项目用。

常见邮箱 SMTP 配置：

| 邮箱 | SMTP_SERVER | SMTP_PORT |
| --- | --- | --- |
| 163 | `smtp.163.com` | `465` |
| QQ | `smtp.qq.com` | `465` |
| Gmail | `smtp.gmail.com` | `587` |
| Outlook | `smtp.office365.com` | `587` |

### 2. 启用 Actions

仓库 → **Actions** 标签页 → 若提示则点击启用。之后点 **Run workflow** 手动触发一次
验证配置，date 建议填一个确定的交易日（如 `20260821`）。

## 本地运行

```bash
pip install -r requirements.txt

python fetch_lhb.py --date 20260821 --no-email   # 抓取 + 生成 lhb_report.html，不发信
python fetch_lhb.py --backfill 60 --no-email     # 回补最近 60 天
python fetch_lhb.py --date 2026-08-21            # 发信（需先配好环境变量）
```

`--no-email` 模式下会在项目根目录生成 `lhb_report.html`，浏览器直接打开就能看效果。

Windows PowerShell 下设置环境变量：

```powershell
$env:EMAIL_SENDER="yourname@163.com"; $env:EMAIL_PASSWORD="授权码"
$env:EMAIL_RECEIVER="a@163.com"; $env:SMTP_SERVER="smtp.163.com"; $env:SMTP_PORT="465"
```

## 注意事项

- **默认取「昨天」的数据**，因为交易日数据次日才完整。因此周一运行时取到的是周日
  （非交易日），脚本会记录日志并正常退出，不发邮件——这是预期行为。
- **数据字段**：`stock_lhb_detail_daily_sina` 返回新浪「每日详情」榜，字段为
  `序号 / 股票代码 / 股票名称 / 收盘价 / 对应值 / 成交量 / 成交额 / 指标`，
  **不含「机构净额」**。榜单按 `成交额` 降序。若后续换用含机构净额的接口
  （如 `ak.stock_lhb_jgmmtj_em`），`AMOUNT_COL_CANDIDATES` 会自动优先用它。
- **同一只股票可能有多行**：命中多个上榜指标就会重复出现。parquet 保留全部原始行，
  展示时按股票代码去重，所以「上榜记录」数会大于「涉及个股」数。
- **HTML 附件大小**：约 10 KB/交易日，默认最多嵌入最近 60 天（`MAX_HISTORY_DAYS`），
  更早的数据仍完整保存在 `data/` 里。实际大小每次运行都会打进日志。
- 部分邮箱对 `.html` 附件会提示风险或要求确认后才能下载，这是客户端的安全策略。
  介意的话可以改用 GitHub Pages 部署 HTML、邮件里只放链接。
- 非交易日、接口无数据时跳过发信，退出码 0（Actions 不会标红）。
- 邮件发送失败只打印错误、不中断脚本；parquet 同时作为 artifact 保留 30 天。
- GitHub Actions 定时任务在高峰期可能延迟数分钟至数十分钟触发，属正常现象。
