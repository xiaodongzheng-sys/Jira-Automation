# Jira Tool Setup For Teammates

给同事的 Jira 工具安装说明

This guide is for teammates who need to run the tool on their own Mac.
这份说明给需要在自己 Mac 上运行这个工具的同事使用。

## What You Need

开始前请确认：

- you are using a Mac
- 你使用的是 Mac
- you can open BPMIS in Chrome
- 你可以在 Chrome 里打开 BPMIS
- your Google account can open the Spreadsheet you need
- 你的 Google 账号可以访问要使用的 Spreadsheet
- Git is installed
- 电脑里已经装好 Git

## Step 1: Get the Code

打开 Terminal，进入你想存放项目代码的文件夹。

Open Terminal and choose a folder where you want to keep the project.

Then run:

```bash
git clone -b codex/team-web-version https://github.com/xiaodongzheng-sys/Jira-Automation.git
cd Jira-Automation
```

这会把团队当前使用的代码下载到你的电脑上。  
This downloads the latest team version of the tool to your Mac.

## Step 2: Start the Tool

推荐方式：直接在 Terminal 里运行下面这些命令。  
Recommended: run the following commands in Terminal.

在同一个 Terminal 窗口里运行：

In the same Terminal window, run:

```bash
./scripts/install_team_helper_local.sh
./scripts/run_team_stack.sh start
```

这些命令会：

- prepare the local environment
- 准备本地运行环境
- start the helper that talks to BPMIS
- 启动和 BPMIS 通信的 helper
- start the local web portal
- 启动本地网页工具

## Step 3: Open the Tool

在 Chrome 打开下面这个地址：

Open this address in Chrome:

[http://127.0.0.1:5000](http://127.0.0.1:5000)

## Step 4: Log In To BPMIS

请确认 Chrome 里已经打开 BPMIS，并且处于登录状态。  
Make sure BPMIS is already open and logged in in Chrome.

如果没有登录，工具就无法创建 Jira。  
If you are not logged in, the tool will not be able to create Jira tickets.

## Step 5: Connect Google

在网页里点击 `Connect Google`。  
On the tool page, click `Connect Google`.

请使用能够访问目标 Spreadsheet 的 Google 账号。  
Use the Google account that can access your Spreadsheet.

你不需要自己去配置 Google Cloud 或 redirect URL。  
You do not need to set up Google Cloud or configure redirect URLs yourself.

## Step 6: Confirm The Status Box

你应该看到：

- `Google Sheets = Connected`
- `Local Helper = Connected`

If both are connected, you can continue.  
如果这两项都显示已连接，就可以继续。

## Step 7: Fill In Your Settings

在网页上填写这些信息：

- the Spreadsheet link
- Spreadsheet 链接
- the Input tab name
- Input 页签名称
- the Issue ID column name
- Issue ID 列名
- the Jira Ticket Link column name
- Jira Ticket Link 列名
- the remaining field mapping values your team uses
- 你们团队约定的其他字段映射

Then click `Save Web Config`.  
然后点击 `Save Web Config`。

## Step 8: Preview First

先点击 `Preview Eligible Rows`。  
Click `Preview Eligible Rows` first.

这一步会先显示哪些行准备好要创建 Jira，不会先盲目执行。  
This shows which rows are ready before any Jira ticket is created.

## Step 9: Run Ticket Creation

如果预览结果没问题，再点击 `Run Ticket Creation`。  
If the preview looks correct, click `Run Ticket Creation`.

## Daily Use

以后每次使用时，只需要：

Each time you use the tool:

1. Open Chrome and log in to BPMIS
1. 打开 Chrome 并登录 BPMIS
2. Go to the project folder in Terminal
2. 在 Terminal 里进入项目目录
3. Run:

```bash
./scripts/run_team_stack.sh start
```

4. Open [http://127.0.0.1:5000](http://127.0.0.1:5000)
4. 打开 [http://127.0.0.1:5000](http://127.0.0.1:5000)
5. Preview first, then run
5. 先 Preview，再 Run

## If Something Does Not Work

## 如果遇到问题

### The page does not open

如果网页打不开，运行：

```bash
./scripts/run_team_stack.sh restart
```

Then reopen:  
然后重新打开：

[http://127.0.0.1:5000](http://127.0.0.1:5000)

### Local Helper shows Offline

如果 `Local Helper` 显示 `Offline`，运行：

```bash
./scripts/run_team_stack.sh restart
```

### BPMIS error

如果看到 BPMIS 相关报错，请确认 Chrome 里的 BPMIS 仍然保持登录。  
Make sure BPMIS is still open and logged in in Chrome.

### Google or Spreadsheet error

请检查：

- your Google account can open the Spreadsheet
- 你的 Google 账号能打开 Spreadsheet
- the Spreadsheet link is correct
- Spreadsheet 链接是否正确
- the tab name is correct
- tab 名称是否正确
- the column header names are correct
- 列名是否正确

### Still stuck

如果还卡住，请把下面这些信息发给维护者：

- a screenshot of the error
- 错误截图
- the row number that failed
- 失败的行号
- what you clicked just before the error happened
- 报错前你点击了什么
