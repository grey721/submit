# Submit CLI

`submit` 是一个面向学校算力平台定制的自动提交命令行工具。

它的目标是让训练任务提交尽可能接近本地运行体验：用户只需要在项目目录下执行 `submit train.py`，工具会自动生成远端启动脚本、提交训练任务、检查资源、回流日志，并在终端中持续显示任务运行状态。

当前版本为 **Submit CLI v3**，主要面向学校训练平台的固定接口和固定资源组使用，不追求通用集群兼容。

---

## 1. 项目特性

- 使用命令行直接提交训练脚本
- 默认将全局配置保存在 `~/.autosubmit/`
- 支持自定义全局配置目录
- 支持账号密码登录
- 支持手动导入已有 Token / Cookie 会话
- 支持验证码获取
- 支持镜像列表查询与镜像绑定
- 支持节点资源列表查询
- 支持 CPU / GPU 数量配置
- 支持 Conda 环境自动激活
- 支持固定脚本参数配置
- 支持前台实时追踪远端日志
- 支持断线后重新连接任务日志
- 支持生成事故报告和 reconnect 命令
- 支持清理本地报告与日志缓存

---

## 2. 安装

### 2.1 克隆仓库

```bash
git clone https://github.com/grey721/submit.git
cd submit
```

### 2.2 安装依赖

```bash
npm install
```

### 2.3 注册为全局命令

```bash
npm link
```

注册完成后，可以直接使用：

```bash
submit --help
```

如果不想注册全局命令，也可以直接运行：

```bash
node /path/to/submit/bin/submit.js <args>
```

---

## 3. 注册、初始化与登录

本章只介绍第一次使用时必须完成的准备工作。

### 3.1 初始化全局配置目录

默认初始化：

```bash
submit init
```

默认会创建：

```text
~/.autosubmit/
├── config.json
├── settings.json
├── session.json
├── reports/
└── launchers/
```

如果希望把配置目录放到其他位置，可以使用：

```bash
submit init --global-dir /private/path/.autosubmit
```

初始化命令会写入 `config.json` 和 `settings.json`，并创建 `reports/` 与 `launchers/` 目录。

### 3.2 配置平台地址

初始化后，打开：

```bash
~/.autosubmit/config.json
```

填写学校算力平台的地址：

```json
{
  "baseURL": "https://your-platform-address",
  "ignoreHTTPSErrors": true,
  "timeoutMs": 15000,
  "credentials": {
    "account": "",
    "password": "",
    "captcha": ""
  }
}
```

其中：

- `baseURL`：学校算力平台 API 地址
- `ignoreHTTPSErrors`：是否忽略 HTTPS 证书错误
- `timeoutMs`：请求超时时间
- `credentials.account`：登录账号
- `credentials.password`：登录密码
- `credentials.captcha`：验证码字段，通常不需要手动填写

### 3.3 登录账号

```bash
submit login --account <账号> --password '<密码>'
```

例如：

```bash
submit login --account 2023000000 --password 'your_password'
```

登录命令会把账号密码写入全局 `config.json`，并清理旧的 session 缓存，然后执行一次登录检查。

### 3.4 验证码

如果平台需要验证码，可以先手动获取验证码图片：

```bash
submit captcha fetch
```

指定输出路径：

```bash
submit captcha fetch --output /tmp/captcha.png
```

如果登录过程中检测到验证码错误，CLI 会保存验证码图片，并在终端提示用户输入。

### 3.5 手动导入已有会话

如果你已经从浏览器或其他方式拿到了平台 Token，可以手动导入：

```bash
submit session import --token <token>
```

如果还需要 Cookie：

```bash
submit session import --token <token> --cookie "JSESSIONID=xxx; other=yyy"
```

也可以同时指定账号：

```bash
submit session import --token <token> --cookie "JSESSIONID=xxx" --account <账号>
```

该命令会写入 `session.json`，并将权限设置为 `600`。

### 3.6 清除登录状态

只清除 session：

```bash
submit session clear
```

清除账号密码和 session：

```bash
submit logout
```

---

## 4. 使用流程

本章介绍正常提交一次训练任务的完整流程。

### 4.1 第一步：初始化并登录

```bash
submit init
submit login --account <账号> --password '<密码>'
```

### 4.2 第二步：查看可用镜像

```bash
submit images
```

该命令会从平台读取镜像列表，并在终端输出可选镜像。

### 4.3 第三步：绑定默认镜像

可以通过序号选择：

```bash
submit set image 1
```

也可以通过镜像名、镜像 ID 或关键词选择：

```bash
submit set image pytorch
```

如果输入的不是完整镜像路径，CLI 会尝试从平台镜像列表中解析对应镜像。`set image` 支持 `image / index / id / keyword` 四种形式。

### 4.4 第四步：设置资源

设置 GPU 数量：

```bash
submit set accelerator 1
```

设置 CPU 核数：

```bash
submit set cpu 4
```

查看当前设置：

```bash
submit get all
```

### 4.5 第五步：设置 Conda 环境

如果远端容器中需要先激活 Conda 环境：

```bash
submit set conda-env pytorch
```

清除 Conda 环境：

```bash
submit set clear-conda-env
```

### 4.6 第六步：进入项目目录并提交训练脚本

```bash
cd ~/your-project
submit train.py --epochs 20
```

执行后，CLI 会自动：

1. 在全局 launcher 目录中生成启动脚本
2. 在启动脚本中 `cd` 回当前项目目录
3. 如果设置了 Conda 环境，则自动执行 `conda activate`
4. 使用对应解释器运行目标脚本
5. 构造学校平台训练任务请求
6. 提交前检查资源
7. 提交任务
8. 在当前终端实时显示远端日志

对于 `.py` 文件，默认使用 `python`；对于 `.sh` / `.bash` 文件，默认使用 `bash`。

### 4.7 第七步：断线后重新连接

任务提交成功后，终端会打印 reconnect 命令。断线后可以使用：

```bash
submit reconnect <handle>
```

例如：

```bash
submit reconnect tid_123456
```

也可以传入任务 ID、任务名或事故报告 JSON：

```bash
submit reconnect <taskId>
submit reconnect <taskName>
submit reconnect ~/.autosubmit/reports/incident-xxx.json
```

---

## 5. 完整功能说明

### 5.1 全局配置目录

默认目录：

```bash
~/.autosubmit/
```

目录结构：

```text
~/.autosubmit/
├── config.json       # 平台地址、账号密码等基础配置
├── settings.json     # 镜像、CPU、GPU、Conda、参数等提交设置
├── session.json      # 登录后的 Token / Cookie 缓存
├── reports/          # 事故报告与 reconnect 信息
└── launchers/        # 自动生成的远端启动脚本
```

也可以通过初始化时指定：

```bash
submit init --global-dir /private/path/.autosubmit
```

或者使用环境变量：

```bash
export AUTOSUBMIT_HOME=/private/path/.autosubmit
```

### 5.2 查看版本与帮助

```bash
submit --help
submit --version
submit -v
```

其中 `submit -v` 会输出版本、入口文件路径、当前全局目录和默认全局目录。

### 5.3 认证相关命令

```bash
submit login --account <account> --password <password>
submit logout
submit session import --token <token> [--cookie "k=v; k2=v2"] [--account <account>]
submit session clear
submit captcha fetch [--output <filePath>]
```

说明：

- `login`：保存账号密码并检查登录
- `logout`：清除账号密码和 session
- `session import`：手动导入 Token / Cookie
- `session clear`：只清除 session
- `captcha fetch`：获取验证码图片

### 5.4 镜像相关命令

```bash
submit images
submit set image <image|index|id|keyword>
submit get image
```

示例：

```bash
submit images
submit set image 2
submit set image pytorch
submit get image
```

### 5.5 节点资源相关命令

```bash
submit nodes
```

该命令会查看训练资源组节点情况，包括节点状态、资源状态、CPU / GPU 使用情况、根分区可用空间等。核心逻辑中会根据 CPU、GPU、节点状态、资源健康状态和根分区空间选择合适节点。

### 5.6 资源配置命令

```bash
submit set accelerator <count>
submit set cpu <cores>
submit get accelerator
submit get cpu
```

示例：

```bash
submit set accelerator 1
submit set cpu 8
submit get all
```

限制：

- `accelerator` 必须是大于等于 0 的数字
- `cpu` 必须是大于等于 1 的数字

### 5.7 任务名配置

设置默认任务名：

```bash
submit set task-name <name>
```

清除默认任务名：

```bash
submit set clear-task-name
```

查看任务名：

```bash
submit get task-name
```

如果不设置任务名，CLI 会自动生成任务名。

### 5.8 日志轮询间隔

设置日志轮询间隔：

```bash
submit set poll-interval 2
```

查看：

```bash
submit get poll-interval
```

默认轮询间隔为 2 秒。

### 5.9 Launcher 脚本

默认情况下，CLI 会在：

```bash
~/.autosubmit/launchers/
```

生成远端启动脚本。

保留 launcher：

```bash
submit set keep-launcher 1
```

不保留 launcher：

```bash
submit set keep-launcher 0
```

查看设置：

```bash
submit get keep-launcher
```

注意：训练容器必须能访问 launcher 所在路径。如果平台只挂载项目目录而不挂载 `~/.autosubmit/launchers/`，任务可能会在执行启动脚本前失败。遇到这种情况，需要调整 `settings.json` 中的 `singleFile.launcherDir`，把 launcher 放到远端容器可见的位置。

### 5.10 Conda 环境

设置 Conda 环境：

```bash
submit set conda-env <env>
```

示例：

```bash
submit set conda-env pytorch
```

清除：

```bash
submit set clear-conda-env
```

CLI 会在启动脚本中尝试寻找常见 Conda 初始化脚本，例如：

- `/opt/conda/etc/profile.d/conda.sh`
- `$HOME/miniconda3/etc/profile.d/conda.sh`
- `$HOME/anaconda3/etc/profile.d/conda.sh`
- `$HOME/miniforge3/etc/profile.d/conda.sh`
- `$HOME/mambaforge/etc/profile.d/conda.sh`

如果找不到 Conda，会报错退出。

### 5.11 固定脚本参数

设置每次提交都会自动附加的参数：

```bash
submit set script-args --config configs/base.yaml
```

之后执行：

```bash
submit train.py --epochs 20
```

实际运行时会把固定参数和本次命令行参数一起传给训练脚本。

清除固定参数：

```bash
submit set clear-script-args
```

查看：

```bash
submit get script-args
```

### 5.12 提交任务

基本形式：

```bash
submit <file> [script args...]
```

示例：

```bash
submit train.py
submit train.py --epochs 20 --batch-size 32
submit scripts/run.sh
```

支持的默认文件类型：

```text
.py      -> python
.sh      -> bash
.bash    -> bash
```

如果文件扩展名没有配置解释器，需要在 `settings.json` 中补充解释器配置。

### 5.13 重连任务日志

```bash
submit reconnect <handle|taskId|taskName|report.json>
```

示例：

```bash
submit reconnect tid_123456
submit reconnect 123456
submit reconnect my-task-name
submit reconnect ~/.autosubmit/reports/incident-xxx.json
```

### 5.14 清理本地报告与日志缓存

```bash
submit clear-logs
```

也可以使用：

```bash
submit logs clear
```

该命令会清空：

```bash
~/.autosubmit/reports/
```

### 5.15 查看当前配置

查看全部：

```bash
submit get all
```

查看单项：

```bash
submit get image
submit get accelerator
submit get cpu
submit get task-name
submit get poll-interval
submit get keep-launcher
submit get conda-env
submit get script-args
submit get launcher-dir
```

---

## 6. 推荐的一次性配置示例

第一次使用可以按下面顺序执行：

```bash
# 1. 初始化
submit init

# 2. 登录
submit login --account <账号> --password '<密码>'

# 3. 查看平台镜像
submit images

# 4. 绑定镜像
submit set image 1

# 5. 查看节点资源
submit nodes

# 6. 设置默认资源
submit set accelerator 1
submit set cpu 4

# 7. 设置 Conda 环境
submit set conda-env pytorch

# 8. 查看当前配置
submit get all

# 9. 进入项目并提交
cd ~/your-project
submit train.py --epochs 20
```

---

## 7. 工作机制

执行：

```bash
submit train.py --epochs 20
```

大致流程如下：

1. CLI 检查本地文件是否存在
2. 根据文件扩展名选择解释器
3. 生成 launcher 脚本
4. launcher 脚本进入当前项目目录
5. 如设置了 Conda 环境，则激活 Conda
6. 运行训练脚本
7. CLI 构造学校算力平台训练任务请求
8. 提交前调用资源检查接口
9. 自动选择合适节点
10. 提交训练任务
11. 前台轮询远端日志
12. 任务完成后输出最终状态

---

## 8. 当前平台固定项说明

当前版本针对学校算力平台定制，部分提交字段在代码中固定，包括：

- `projectId`
- `resGroupId`
- `imageType`
- `type`
- `switchType`
- GPU 卡型号：`NVIDIA-A800-80GB-PCIe`
- 训练资源组名称：`training`

动态配置项主要包括：

- 镜像
- CPU 核数
- GPU 数量
- 任务名
- 启动命令
- Conda 环境
- 脚本参数

这意味着该工具更适合作为学校内部算力平台的专用 CLI，而不是通用 Slurm / Kubernetes / 云平台提交器。

---

## 9. 常见问题

### 9.1 提示没有初始化配置

报错类似：

```text
Global config not found. Run `submit init` first.
```

解决：

```bash
submit init
```

### 9.2 提示缺少账号或密码

解决：

```bash
submit login --account <账号> --password '<密码>'
```

或者手动检查：

```bash
~/.autosubmit/config.json
```

### 9.3 提示没有设置镜像

先查看镜像：

```bash
submit images
```

再设置镜像：

```bash
submit set image 1
```

### 9.4 远端找不到 launcher 脚本

原因通常是训练容器无法访问本地全局目录：

```bash
~/.autosubmit/launchers/
```

解决方式：

1. 确认平台是否挂载了该路径
2. 如果没有，修改 `settings.json` 中的 `singleFile.launcherDir`
3. 将 launcher 目录设置到远端容器可见的项目目录下

### 9.5 Conda 激活失败

请确认远端容器中存在 Conda，并且环境名正确：

```bash
submit set conda-env <正确的环境名>
```

如果不需要 Conda：

```bash
submit set clear-conda-env
```

### 9.6 任务断线了怎么办

使用提交时打印的 reconnect 命令：

```bash
submit reconnect <handle>
```

或者从事故报告恢复：

```bash
submit reconnect ~/.autosubmit/reports/incident-xxx.json
```

---

## 10. 卸载

如果使用 `npm link` 安装：

```bash
npm unlink -g submit
```

如果是在仓库目录中执行过 `npm link`，也可以在仓库目录中执行：

```bash
npm unlink
```

如果希望同时删除全局配置和缓存：

```bash
rm -rf ~/.autosubmit
rm -f ~/.autosubmit-home
```

---

## 11. 打包

在项目根目录执行：

```bash
npm pack
```

会生成类似：

```bash
submit-3.0.0.tgz
```

---

## 12. 注意事项

- 本工具是学校算力平台专用 CLI，不保证适配其他平台。
- 当前版本依赖学校平台现有 API 字段和资源组配置。
- 登录信息会保存在本地 `config.json` 中，请注意文件权限和账号安全。
- `session.json` 中会缓存 Token / Cookie，建议不要上传到 Git。
- `.autosubmit/`、`session.json`、`reports/`、`launchers/` 等本地状态文件不应提交到仓库。
- 如果多人共用机器，建议使用 `submit init --global-dir <private_path>` 将配置目录放到自己的私有目录下。
