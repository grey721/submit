# submit cli

学校算力平台专用 CLI。体验目标是“像本地跑脚本一样提交远端训练任务”，日志直接回流到当前终端，`Ctrl+C` 会删除远端任务。

当前版本已经去掉“历史任务模板提交”依赖，提交 payload 直接按已抓到的网页请求硬编码构造，并在提交前调用资源检查接口。

## 特性

- 全局配置默认放在 `~/.autosubmit/`
- 支持 `submit init --global-dir <path>` 把全局状态迁到自定义目录
- 启动脚本默认生成在当前项目目录下 `./.autosubmit/launchers/`
- 提交前自动做 `/api/iresource/v1/train/check-resources`
- 提交后前台轮询 `/read-log`，终端实时显示日志
- 镜像拉取阶段会打印节点信息
- 拉取失败若命中 `no space left on device`，会在本次提交内自动切到下一台更健康的节点重试
- `Ctrl+C` 自动删除远端任务
- 断线后可用 `submit reconnect ...` 继续追日志
- Session 默认缓存 12 小时

## 目录

全局状态默认在：

```text
~/.autosubmit/
├── config.json
├── settings.json
├── session.json
└── reports/
```

项目内临时脚本：

```text
<your-project>/.autosubmit/launchers/
```

## 安装

```bash
cd submit
npm install
npm link
```

或直接运行：

```bash
node /path/to/submit/bin/submit.js <args>
```

## 卸载

如果你是用 `npm link` 安装的：

```bash
npm unlink -g submit
```

如果当时就在 `submit/` 目录执行了 `npm link`，也可以在该目录执行：

```bash
cd submit
npm unlink
```

如果还想一起删除全局缓存目录：

```bash
rm -rf ~/.autosubmit
```

## 初始化

```bash
submit init
submit init --global-dir /private/path/.autosubmit
```

会写入：

- `<global-dir>/config.json`
- `<global-dir>/settings.json`

`config.json` 现在只保留最小配置：

```json
{
  "baseURL": "https://10.10.24.107:32206",
  "ignoreHTTPSErrors": true,
  "timeoutMs": 15000,
  "credentials": {
    "account": "",
    "password": "",
    "captcha": ""
  }
}
```

## 快速开始

```bash
submit init
submit init --global-dir /private/path/.autosubmit
submit login --account xxxxxxxxxx --password '******'
submit images
submit set image 1
submit set accelerator 1
submit set cpu 4

cd ~/your-project
submit train.py --epochs 20
```

## 常用命令

认证：

```bash
submit login --account <account> --password <password>
submit logout
submit session import --token <token> [--cookie "k=v;..."] [--account <account>]
submit session clear
submit captcha fetch [--output /tmp/captcha.png]
```

配置：

```bash
submit set image <image|index|id|keyword>
submit set accelerator <count>
submit set cpu <cores>
submit set task-name <name>
submit set clear-task-name
submit set poll-interval <seconds>
submit set keep-launcher <0|1>
submit set script-args <args...>
submit set clear-script-args
submit get <key|all>
```

提交与追踪：

```bash
submit <file> [script args...]
submit reconnect <handle|taskId|taskName|report.json>
submit clear-logs
```

## 运行方式

`submit train.py --epochs 20` 的执行过程：

1. 在当前目录生成 `./.autosubmit/launchers/<file>.<timestamp>.submit.sh`
2. 启动脚本内容固定为 `cd <cwd> && <interpreter> <abs_file> <args>`
3. 用硬编码字段直接构造 `POST /api/iresource/v1/train`
4. 提交前先调用 `/api/iresource/v1/train/check-resources`
5. 提交成功后前台轮询 `/api/iresource/v1/train/{id}/read-log`

## 硬编码说明

当前版本不再从历史任务复制字段。以下提交字段直接固定在代码里：

- `projectId`
- `resGroupId`
- `imageType`
- `type`
- `switchType`
- GPU 卡型号 `NVIDIA-A800-80GB-PCIe`

动态项只有：

- 镜像
- CPU 核数
- GPU 数量
- 任务名
- 启动命令

## 节点自动选择

- CLI 会实时查询 training 资源组的节点详情
- 先硬筛选出满足本次申请 CPU/GPU 数量的节点
- 再要求节点状态为 `ready`、资源状态为 `healthy`，且根分区可用空间大于 0
- 在满足硬条件的节点里，选择根分区可用空间最大的节点
- 如果这个最佳节点仍然因为 `no space left on device` 拉取失败，CLI 会直接结束并提示稍后再试

前台日志顺序固定为：

```text
Reconnect command: ...
ImagePulling -> Node: gpu09, CPU: 37/56, GPU: 4/6, RootAvail: 166.7G (19%)
Submit succeeded
Task completed: status=Succeeded
```

## Launcher 说明

默认 launcher 放在当前项目里而不是 `~/.autosubmit/`，原因是远端训练容器必须能看到这个脚本路径。

如果要保留生成的脚本：

```bash
submit set keep-launcher 1
```

## 验证码

If the platform requires a captcha, `submit login` now fetches the captcha image automatically by default.

You can still fetch it manually:

```bash
submit captcha fetch
submit login
```

```bash
submit login --account <account> --password <password>
```

When a captcha error is detected, the CLI saves the captcha image to the current directory and prompts for the code in the terminal.

## 打包

在 `submit/` 目录下执行：

```bash
npm pack
```

会生成类似：

```bash
submit-<version>.tgz
```

## 注意

- 这是针对当前学校平台接口定制的版本，不做旧版兼容。
- `submit init` 直接写默认配置，不再复制任何模板文件。
- `submit init --global-dir <path>` 会把后续命令使用的全局状态目录切到该路径；不传时默认仍是 `~/.autosubmit/`。
- `submit set script-args ...` 会作为固定参数追加到每次提交的脚本参数前面。
- 如果镜像绑定没设置，提交会直接失败并提示先执行 `submit set image ...`。
