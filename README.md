# A股龙虎榜每日自动化获取与看板推送

用 Python + GitHub Actions 每个交易日自动抓取龙虎榜数据，存为 Parquet，生成可交互看板
部署到 GitHub Pages，并邮件推送当日榜单 + 看板链接。**完全免费，无需任何 Token**
（数据源为 akshare 抓取的东方财富公开页面，自带涨跌幅、龙虎榜净买额、上榜原因）。

📊 **看板地址**：https://cleo-zou.github.io/lhb-data-pipeline/

## 工作流程

```
GitHub Actions (工作日北京时间 16:00)
  ├─ fetch_lhb.py --no-email
  │    ├─ 先查 data/lhb_YYYYMMDD.parquet —— 有就直接用，不重复请求
  │    ├─ 没有才调 akshare 抓取（失败重试 3 次，间隔 5 秒）→ 清洗 → 落盘
  │    └─ 读取本地全部历史 parquet → 生成 output/index.html
  ├─ 把 data/ 提交回仓库，历史就此累积
  ├─ 部署 output/ 到 GitHub Pages
  └─ fetch_lhb.py --email-only --page-url <部署地址>
       └─ 发邮件：当日榜单表格 + 看板链接 + 当日 parquet
```

邮件必须排在部署之后，才拿得到 `steps.deployment.outputs.page_url`。这一步用
`--email-only` 复用前面已落盘的 parquet，不会重复请求东方财富。

## 看板能做什么

- **选任意历史日期**：日历控件直接挑，也能用下拉列表或 ←/→ 逐个交易日翻。
  选到周末或节假日会自动跳到最近的已归档交易日并给出提示。
- **按列排序**：点表头，数值列按数值排、文本列按拼音排
- **涨跌幅着色**：涨红跌绿，一眼看出当天涨跌方向
- **关键词筛选**：股票名称 / 代码 / 上榜原因都能搜
- 自适应深浅色，手机上也能看

### 页面结构

整个看板是**一个自包含的 `output/index.html`**，所有数据内嵌，页面零网络请求。
3 年历史压到约 2.6 MB（列式编码 + 股票名/上榜原因去重），GitHub Pages 再 gzip 到约 1.1 MB。

```
output/
  index.html            # 唯一的产物：数据全在内，没有任何子资源请求
```

为什么做成单文件：之前拆成「外壳 + 按日 JSON、按需 fetch」，架构上更轻，但在部分
网络环境下，浏览器对 `data/*.json` 的 fetch 会被代理/安全软件拦截，导致「读取
manifest.json 失败」。单文件版只加载 `index.html` 一个文件，不存在可被拦截的子资源，
`file://` 双击打开、离线、换网络都可用。代价是首屏约 1 MB（gzip），桌面端无感。

## 邮件里有什么

| 部分 | 内容 |
| --- | --- |
| **正文** | 当日前 10 名的 HTML 表格（含涨跌幅、龙虎榜净买额）+ 一个跳转看板的按钮，纯内联样式，各家邮箱都能渲染 |
| **纯文本兜底** | 不渲染 HTML 的客户端会看到前 5 名摘要和看板地址 |
| **附件 `lhb_YYYYMMDD.parquet`** | 当日原始数据，约 14 KB，给 pandas 用 |

整封信约几十 KB。之前试过把可交互 HTML 直接当附件发，但邮件客户端会剥掉 `<script>`
导致正文里没法交互，而部分邮箱又对 `.html` 附件提示风险——改用 Pages 就都绕开了。

## 历史数据怎么累积

GitHub Actions 的 runner 每次都是全新的临时环境，`data/` 不会自己保留。这里用了两层
（参考 `Index_Enhancement_Monitor` 的做法）：

1. **提交回仓库**（主）：每次运行后 `git add data/ && git commit && git push`，
   历史数据随仓库一起版本化。单日 parquet 约 14 KB，3 年约 12 MB，完全不占地方。
2. **`actions/cache`**（兜底）：万一某次 push 撞上冲突没提交成功，下次运行仍能从缓存
   恢复。key 用 `github.run_id` 保证每次写新条目，`restore-keys` 前缀匹配拿回最近一份。

脚本侧有两本账，保证同一天永远只问东方财富一次：

- **`data/lhb_YYYYMMDD.parquet`** —— 有数据的日子。`load_or_fetch()` 见到就直接读。
- **`data/_no_data.json`** —— 非交易日。这类日子不产生 parquet，不单独记一笔的话，
  每次回补都会把三年里近千个周末和节假日重新请求一遍。

### 攒历史

仓库里已归档约 3 年。要往更早补就跑一次回补（数据源可回溯到 2010 年）：

Actions → **LHB Daily** → Run workflow → **backfill** 填天数 → 运行。
已归档的日期会瞬间跳过，只请求缺的那些，完成后提交回仓库。

本地跑也一样：

```bash
python fetch_lhb.py --backfill 1095 --no-email    # 近 3 年
```

回补按日期范围批量请求（每段 90 天，东方财富支持一次取一个范围），3 年约 13 次请求、
几分钟完成，比逐日循环快得多。中途 Ctrl+C 也不会白跑——已抓到的都落了盘，
非交易日台账每段落一次盘，重跑会接着上次继续。

## 每日自检

每次运行（生成看板那一步）都会对全量历史做一遍健康自检，结果写进看板顶部、邮件正文，
异常时邮件主题带「自检异常」。检查项：

- **字段合法性**：每个 parquet 是否缺必列、涨跌幅是否全空、股票代码是否有缺失
- **最新日期新鲜度**：最新交易日距今超过 7 天会提示「可能已停更」
- **归档连续性**：归档范围内是否有工作日缺口（非交易日在台账里，不算缺口）

自检通过时看板顶部显示绿色「✅ 自检通过」，邮件主题不带标记；有异常则看板标黄、
邮件主题变 `【龙虎榜·自检异常】…`，并在正文列明具体问题。

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

要用 **Repository secrets**，不是 Environment secrets——后者要求 job 里声明和它同名的
`environment:`，本 workflow 的 `environment` 是 `github-pages`，读不到别的环境的 secret
（表现为「缺少环境变量，跳过邮件发送」）。

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

### 2. GitHub Pages

workflow 里 `configure-pages` 带了 `enablement: true`，首次运行会自动开启 Pages，
一般不用手动设置。如果报权限错，去 Settings → Pages → Source 手动选
**GitHub Actions** 再重跑。

Pages 对 public 仓库免费；private 仓库需要 GitHub Pro。本仓库是 public，没问题。

### 3. 启用 Actions 并试跑

仓库 → **Actions** 标签页 → 若提示则点击启用 → 点 **Run workflow** 手动触发一次，
date 建议填一个确定的交易日（如 `20260821`）验证全流程。

## 本地运行

```bash
pip install -r requirements.txt

python fetch_lhb.py --date 20260821 --no-email   # 抓取 + 生成 output/index.html
python fetch_lhb.py --backfill 60 --no-email     # 回补最近 60 天
python fetch_lhb.py --date 2026-08-21            # 抓取 + 生成 + 发信（需配好环境变量）
python fetch_lhb.py --email-only --page-url URL  # 只发信，复用已有 parquet
```

看板是自包含单文件，**双击 `output/index.html` 就能直接打开**，也可以起个服务再访问：

```bash
cd output && python -m http.server 8000    # 然后访问 http://localhost:8000
```

Windows PowerShell 下设置环境变量：

```powershell
$env:EMAIL_SENDER="yourname@163.com"; $env:EMAIL_PASSWORD="授权码"
$env:EMAIL_RECEIVER="a@163.com"; $env:SMTP_SERVER="smtp.163.com"; $env:SMTP_PORT="465"
```

## 注意事项

- **默认取「昨天」的数据**，因为交易日数据次日才完整。因此周一运行时取到的是周日
  （非交易日），这天**看板照常重新部署**（历史不变），但不发邮件——这是预期行为。
- **数据字段**：`stock_lhb_detail_em`（东方财富）返回
  `股票代码 / 股票名称 / 收盘价 / 涨跌幅 / 龙虎榜净买额 / 换手率 / 上榜原因`，
  另有 `龙虎榜买入额 / 卖出额 / 成交额` 一并存进 parquet。看板按 `龙虎榜净买额`
  降序。相比新浪接口，东方财富多了「涨跌幅」，且「上榜原因」对创业板/科创板
  （20% 板）股票也完整返回——新浪对这些票给 NaN。
- **同一只股票可能有多行**：命中多个上榜原因就会重复出现。parquet 保留全部原始行，
  展示时按股票代码去重，所以「上榜记录」数会大于「涉及个股」数。
- **看板体积**：自包含单文件，3 年历史约 2.6 MB（gzip 约 1.1 MB）。归档每多一年约加
  850 KB。实际大小每次运行都会打进日志。
- **Pages 是公开的**：public 仓库的 Pages 任何人都能访问。龙虎榜本身是公开数据，
  但要知道这一点。
- 非交易日、接口无数据时跳过发信，退出码 0（Actions 不会标红）。
- 邮件发送失败只打印错误、不中断脚本；parquet 和看板同时作为 artifact 保留 30 天。
- Pages 相关 action 目前锁在 `configure-pages@v5` / `upload-pages-artifact@v4` /
  `deploy-pages@v4`，和 `Index_Enhancement_Monitor` 一致。上游已有更新的大版本
  （v6/v5/v5），需要时再升。
- GitHub Actions 定时任务在高峰期可能延迟数分钟至数十分钟触发，属正常现象。
