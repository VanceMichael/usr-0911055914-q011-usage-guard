# 亚太 AI 用量成本护栏

护栏服务把调用用量与合同价格版本关联，记录限速或拒绝决定。数据库、缓存和接口配置通过 Compose 环境变量提供，`contracts` 保存事件样例。

启动：`docker compose up --build`；检查：`curl http://localhost:8080/health`。
