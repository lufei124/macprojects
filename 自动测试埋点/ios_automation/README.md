# iOS 埋点自动化（黑盒）

该目录用于连接 iOS 真机，通过 Appium 触发操作后，复用现有 `/events` 接口验证埋点。

## 1. 安装依赖

```bash
pip install -r ios_automation/requirements.txt
```

## 2. 启动依赖服务

- 启动当前项目后端：`python backend_app.py`（默认 `http://127.0.0.1:8888`）
- 启动 Appium Server（默认 `http://127.0.0.1:4723`）
- 连接 iOS 真机并完成 WebDriverAgent 调试准备

## 3. 配置运行参数

通过环境变量传入：

- `TRACKING_DEVICE_ID`：埋点查询的 device_id（必填）
- `TRACKING_ENV`：`test` 或 `uat`
- `EVENT_BASE_URL`：埋点服务地址，默认 `http://127.0.0.1:8888`
- `APPIUM_SERVER_URL`：默认 `http://127.0.0.1:4723`
- `IOS_CAPABILITIES_JSON`：Appium iOS capabilities JSON 字符串

示例：

```bash
export TRACKING_DEVICE_ID="36D2F2D1-524E-4DDB-9CD1-DF06C32E1404"
export TRACKING_ENV="uat"
export IOS_CAPABILITIES_JSON='{"platformName":"iOS","appium:automationName":"XCUITest","appium:udid":"<YOUR_UDID>","appium:bundleId":"com.example.game","appium:noReset":true}'
```

## 4. 执行

```bash
python -m ios_automation.runner --cases ios_automation/cases.yaml
```

只跑某一条：

```bash
python -m ios_automation.runner --case-id cold_start_event
```

## 5. 失败分类

- `UI_ACTION_FAILED`：元素定位/点击失败
- `EVENT_QUERY_FAILED`：`/events` 查询失败
- `EVENT_TIMEOUT`：超时未拉到目标事件
- `FIELD_ASSERTION_FAILED`：事件到了但字段断言失败
