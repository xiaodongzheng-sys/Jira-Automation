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
- your Google account can sign in to the portal if Google login is enabled
- 如果启用了 Google 登录，你的 Google 账号可以登录门户
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
./scripts/run_team_stack.sh start
```

这些命令会：

- prepare the local environment
- 准备本地运行环境
- start the local web portal
- 启动本地网页工具

如果脚本提示 `GOOGLE_OAUTH_CLIENT_SECRET_FILE` 未配置，请先打开项目目录里的 `.env` 文件，填上你本机上的 Google OAuth client secret JSON 路径。  
同时请在 `.env` 里配置 `BPMIS_API_ACCESS_TOKEN`。  
If the script says `GOOGLE_OAUTH_CLIENT_SECRET_FILE` is not configured, open the `.env` file in the project folder and set it to the Google OAuth client secret JSON path on your Mac.  
Also set `BPMIS_API_ACCESS_TOKEN` in `.env`.

## Step 3: Open the Tool

在 Chrome 打开下面这个地址：

Open this address in Chrome:

[http://127.0.0.1:5000](http://127.0.0.1:5000)

## Step 4: Log In To BPMIS

请先在 BPMIS 里生成一个 access token，并填到项目目录的 `.env` 文件里。  
Generate a BPMIS access token first, then save it into the project's `.env` file.

如果没有 token，工具就无法直接调用 BPMIS API 创建 Jira。  
Without a token, the tool cannot call BPMIS APIs directly to create Jira tickets.

## Step 5: Connect Google

在网页里点击 `Connect Google`。
On the tool page, click `Connect Google`.

请使用你的 `@npt.sg` Google 账号。
Use your `@npt.sg` Google account.

你不需要自己去配置 Google Cloud 或 redirect URL。
You do not need to set up Google Cloud or configure redirect URLs yourself.

## Step 6: Confirm The Status Box

你应该看到：

- `BPMIS API` passes in Self-Check

If BPMIS API passes, you can continue.
如果 BPMIS API 通过，就可以继续。

## Step 7: Fill In Your Settings

在网页上填写这些信息：

- the BPMIS API token
- BPMIS API token
- PM Team
- PM 团队
- routing and owner defaults if your team defaults are not prefilled
- 如果团队默认值没有预填，请填写路由和负责人配置

Then click `Save Web Config`.
然后点击 `Save Web Config`。

## Step 8: Sync BPMIS Projects

先进入 `My Projects`，点击 `Sync BPMIS Projects`。
Open `My Projects`, then click `Sync BPMIS Projects`.

这一步会把你的 BPMIS 项目同步到门户自己的项目列表里。
This syncs your BPMIS projects into the portal-owned project list.

## Step 9: Create Jira

从项目卡片里选择组件并创建 Jira。
Choose components from the project card and create Jira tickets.

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

### BPMIS API check fails

如果 Self-Check 里的 `BPMIS API` 失败，运行：

```bash
./scripts/run_team_stack.sh restart
```

### BPMIS error

如果看到 BPMIS 相关报错，请确认 `.env` 里的 `BPMIS_API_ACCESS_TOKEN` 仍然有效。  
If you see BPMIS errors, make sure `BPMIS_API_ACCESS_TOKEN` in `.env` is still valid.

### Google sign-in error

请检查：

- you are using the correct Google account
- 你使用的是正确的 Google 账号
- the portal owner has allowed your domain or email
- 门户管理员已经允许你的域名或邮箱

### Still stuck

如果还卡住，请把下面这些信息发给维护者：

- a screenshot of the error
- 错误截图
- the row number that failed
- 失败的行号
- what you clicked just before the error happened
- 报错前你点击了什么
