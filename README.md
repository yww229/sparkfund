# Spark Python 项目化开发 Demo

基金买入逻辑回归的运行、数据口径及验收见 [机器学习 Demo](ML_DEMO.md)。

两位开发者用各自的编辑器和 Git 分支，在同一个 Spark Standalone 集群上提交、联调和优化作业。示例采用模拟公募基金申购记录，交付包含可直接提交的部署包。

## 1. 团队怎么开发和运行

| 位置 | 负责的事 |
| --- | --- |
| 开发者电脑 | 用 PyCharm、Trae、WorkBuddy 或 VS Code 打开 Git 仓库，各自在分支上修改 |
| 148 提交机 | 通过 SSH 进入各自的工作目录，加载本地配置，统一执行 `scripts/run.sh` |
| Spark Master | 两个 `spark-submit` 使用同一个 Master 地址。各自的应用 ID 可在 Spark UI 查到 |
| 测试 MySQL | 共享固定测试数据。每次执行生成新的 `run_id`，结果、日期范围、运行方式、代码版本和 Spark 应用 ID 均可回查 |

在 148 上使用 Standalone `client` 模式，Driver 随提交进程运行，由共享集群调度 Executor。Python 模块通过 `--py-files` 分发，Connector/J 通过 `--jars` 分发。演示默认每个作业最多用一个 Executor 核，便于在资源充足时同时提交。 [Spark 2.4.3 应用提交](https://archive.apache.org/dist/spark/docs/2.4.3/submitting-applications.html) · [Standalone 资源调度](https://archive.apache.org/dist/spark/docs/2.4.3/spark-standalone.html#resource-scheduling)

## 2. 代码写在哪

```text
spark-project-fund-ml/
├── app.py                          # 读取参数、创建 SparkSession、按注册表分发作业
├── spark_project/
│   ├── settings.py                 # 日期、schema、连接参数与提交参数校验
│   ├── tables.py                   # 所有物理表名常量，避免字符串散落
│   ├── registry.py                 # JobSpec 注册表：作业、目标表、能力开关
│   ├── mysql.py                    # 公用 JDBC 读取、写入、回读和运行登记
│   ├── fund_data.py                # 共用源数据筛选与日期条件下推
│   ├── runner.py                   # ETL 执行：读源、算结果、写回、登记
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── customer_fund.py         # 按客户与基金统计
│   │   └── daily_fund_type.py       # 按日期与基金类别统计
│   └── fund_ml/
│       ├── import_far_trans.py      # 校验并导入 FAR-Trans
│       ├── samples.py               # 构建时间对齐样本与 30 天标签
│       ├── model.py                 # 逻辑回归、选阈值、保存、重载与校验和
│       └── job.py                   # 回写预测、指标和运行登记
├── tests/                          # 纯 Python 单测，不需要 Spark 或 MySQL
├── config/demo.env.example         # 连接、日期和运行时模板
├── scripts/                        # 建库、提交、回查、清理、报告、打包
├── sql/                            # 表结构、模拟数据、FAR-Trans、核验、清理、报告
├── logs/                           # 每次运行的日志，按 run_id 命名
├── data/                           # 真实部署放置 FAR-Trans.zip
├── NOTICE                          # FAR-Trans 来源、许可和固定校验值
└── REVISION                        # 无 Git 检出时使用的基础版本号
```

作业函数接收 DataFrame 并返回结果。新增作业只需三步：在 `jobs/` 写计算函数，在 `registry.py` 增加一条 `JobSpec`（作业名、目标表、是否允许 `preagg`），在 `tables.py` 增加表名并补 SQL。命令行参数由 `app.py` 校验和分发，执行、写回、回读与登记由 `runner.py`（ETL）或 `fund_ml/job.py`（ML）统一处理，不需要再按作业名写分支。

## 3. MySQL 和验收数据

初始化脚本创建独立库，库名由 `DEMO_MYSQL_SCHEMA` 决定，默认 **`spark_project_demo`**。写入 10 位模拟客户、6 只基金和 1,540 条交易，基金覆盖三种类别。脚本可重复执行，已有行会保留。默认统计区间取自 `DEMO_DATE_FROM` / `DEMO_DATE_TO`，模板值为 `2025-01-06` 至 `2025-01-31`，两端均包含；`run.sh` 也接受命令行日期覆盖。

| 表 | 行粒度 | 内容 |
| --- | --- | --- |
| `demo_customer` | 一位客户 | 客户编号、名称、客群 |
| `demo_fund` | 一只基金 | 基金编号、名称、类别 |
| `demo_fund_trade` | 一笔交易 | 流水号、客户、基金、确认日期、业务类型、状态、金额。客户和基金通过外键关联 |
| `customer_fund_summary` | 一次运行的一组客户和基金 | 申购笔数、金额、最近一次确认日期、客群、基金类别 |
| `daily_fund_type_summary` | 一次运行的一个日期和基金类别 | 申购笔数和金额 |
| `demo_run` | 一次完成的运行 | 作业、日期范围、策略、结果行数、Spark 应用 ID、代码版本和完成时间 |

两个作业采用同一业务口径。交易在统计期内，类型为申购，状态已确认，金额大于零。`C001/F001` 在 1 月有两笔符合条件的申购，金额为 100 和 200。同期失败、待确认及赎回交易不计入，2 月 3 日的 400 元申购在切换到 2 月统计时才计入。

执行成功时先用子查询做列裁剪和日期过滤，把扫描留在 MySQL；结果写入后按 `run_id` 回读行数，一致后再登记 `demo_run`。提交前会检查 `run_id` 是否已出现在结果表或登记表，重复编号直接拒绝。每次运行把完整控制台输出写到 `logs/<run_id>.log`，路径记入登记表。若执行中断，可用日志里的 `run_id` 检查已写入的行，必要时用 `scripts/cleanup_run.sh` 清理后重跑。重新提交默认生成新编号，也可用 `DEMO_RUN_ID` 指定。

## 4. 148 环境准备和首跑

用公司已开通的方式 SSH 登录 148，确认 `java -version` 为 1.8、`spark-submit --version`、`command -v mysql`，以及服务器上的 Connector/J JAR 路径。Driver 和 Executor 都需要能访问测试 MySQL。模板使用已知的 `spark://172.31.0.2:7077`，按现场实际地址核对。 [Spark 2.4.3 JDBC](https://archive.apache.org/dist/spark/docs/2.4.3/sql-data-sources-jdbc.html)

### 4.1 Python 运行时

Spark 2.4.3 只支持 Python 3.4–3.7，而 148 的系统 Python 是 3.12，只能用于打包模块和导入数据（`DEMO_PACK_PYTHON`），不能跑 PySpark 作业。本机不使用 conda，也不用共享的 `/data/gxp/python368`。

| 解释器 | 结论 |
| --- | --- |
| `/data/gxp/python368`（3.6.8） | 已否决。裸跑 `import ctypes` 直接 `SIGSEGV(139)`：`_ctypes.so` 与 glibc 2.35 / libffi 3.4 不兼容，Executor 端的 `pyspark.daemon` 起不来，ML 作业不可用 |
| `/data/spark-python37/bin/python3`（3.7.12） | 当前使用。从 `/data/pybuild/Python-3.7.12` 源码编译安装，`import ctypes` 正常 |

该 3.7 编译时未带 `_ssl`，不能自行 `pip install`。NumPy 1.21.6（支持 3.7 的最后一个大版本）用任一可联网的 Python 3 下载 `numpy-1.21.6-cp37-cp37m-manylinux*.whl`，解包到独立 site 目录：

```bash
mkdir -p /data/spark-project-fund-mlnew/py37-site
python3 -m zipfile -e numpy-1.21.6-cp37-cp37m-manylinux*.whl /data/spark-project-fund-mlnew/py37-site
```

驱动和 Executor 必须同时看到这个目录。`demo.env` 用 `PYTHONPATH` 指定，`scripts/run.sh` 会额外补一条 `--conf spark.executorEnv.PYTHONPATH=$PYTHONPATH`；缺这条时 Executor 会在 `pyspark.ml.linalg` 上报 `No module named 'numpy'`。

`spark-defaults.conf` 全局把 `spark.pyspark.python` 钉在 3.6.8，所以 `run.sh` 必须用 `--conf` 覆盖，不能依赖默认值。

### 4.2 配置和首跑

将 `spark-project-fund-ml.zip` 上传到服务器，解压到目标目录；Git 用户直接进入自己的克隆。本机部署目录为 `/data/spark-project-fund-mlnew`。

```bash
unzip spark-project-fund-ml.zip
cd spark-project-fund-ml        # Git 克隆则进入克隆目录，如 /data/spark-project-fund-mlnew
cp -n config/demo.env.example demo.env
nano demo.env
```

| 配置项 | 填写内容 |
| --- | --- |
| `SPARK_MASTER` | Spark Master 的 `spark://` 地址 |
| `SPARK_MYSQL_JAR` | 148 上兼容的 Connector/J JAR 绝对路径 |
| `PYSPARK_PYTHON`、`PYSPARK_DRIVER_PYTHON` | 同一份 3.7 解释器的绝对路径，模板为 `/data/spark-python37/bin/python3` |
| `PYTHONPATH` | NumPy 所在 site 目录，模板为 `/data/spark-project-fund-mlnew/py37-site` |
| `DEMO_MODEL_ROOT` | `fund_purchase_lr` 的模型输出根目录，Driver 与 Executor 都要可写 |
| `DEMO_MYSQL_HOST`、`DEMO_MYSQL_PORT` | 测试 MySQL 地址及端口，模板端口为 3306 |
| `DEMO_MYSQL_SCHEMA` | 库名，模板为 `spark_project_demo_new`；多人共用服务器时各用各的库名 |
| `DEMO_MYSQL_SETUP_USER` | 建库账号，需目标库的 `CREATE`、`REFERENCES`、`SELECT`、`INSERT`、`UPDATE` |
| `DEMO_MYSQL_USER` | 作业账号，需源表 `SELECT`，结果表及登记表 `SELECT`、`INSERT` |
| `DEMO_DATE_FROM`、`DEMO_DATE_TO` | 默认统计区间，两端包含；命令行传入日期时以命令行为准 |
| `DEMO_RUN_ID` | 可选。清理某个 `run_id` 后复用同一编号重跑，不设置则每次生成 UUID |
| `DEMO_NEGATIVES_PER_POSITIVE` | 可选。ML 每个正例保留的负例数，默认 20 |

连接配置留在各自的 `demo.env` 中。运行时输入密码。首次初始化由负责建库的人执行一次。若账号由 DBA 持有，可请 DBA 执行 `sql/setup.sql` 并授予上表的表级权限。

```bash
source demo.env
bash scripts/doctor.sh          # 只读自检，全项 OK 再继续
bash scripts/setup_mysql.sh
bash scripts/run.sh customer_fund
```

想一次跑完整条链路（自检 → 建库 → 导入 FAR-Trans → ETL → ML → 两次核验），执行 `bash scripts/demo_all.sh`，末行出现 `DEMO COMPLETE` 即全部通过。

提交后复制控制台 `VERIFY_COMMAND=` 后的命令执行。`WRITE_AND_READBACK=PASS` 表示 Spark 已回读条数并登记运行，MySQL 验收末行 `PASS` 表示独立 SQL 重算也通过。还可以把日期改成 `2025-02-03 2025-02-03` 再跑一次，检查 `C001/F001` 变为 **1 笔、400 元、最近确认日 2 月 3 日**。

```sql
-- 用本次真实运行编号替换 RUN_ID
SELECT r.spark_app_id, r.code_version, s.customer_id, s.fund_id,
       s.subscription_count, s.subscription_amount, s.last_confirm_date
FROM customer_fund_summary s JOIN demo_run r ON r.run_id = s.run_id
WHERE s.run_id = 'RUN_ID' AND s.customer_id = 'C001' AND s.fund_id = 'F001';
```

## 5. 两个人如何演示开发和合并

两人在各自分支开发，主分支合并。示例分工：

| 分支 | 内容 | 验收 |
| --- | --- | --- |
| `demo/baseline` | 共用工程加首个客户基金作业 | 查 `C001/F001`，1 月 2 笔、300 元 |
| `demo/customer-last-date` | 开发者 A 扩展原作业和核验 SQL | 同一数据另存最近确认日 `2025-01-08` |
| `demo/daily-fund-type` | 开发者 B 新增日汇总与登记 | 用同一入口提交 `daily_fund_type`，出具独立核验 `PASS` |
| `main` | 两条分支合并后的版本 | 两条作业各重新提交一次，均通过回查 |

开发者 A 在自己的分支修改并推送：

```bash
git switch -c demo/customer-last-date
# 修改作业与核验 SQL
git commit -am "feat: add last confirm date"
git push -u origin demo/customer-last-date
```

开发者 B 并行创建 `demo/daily-fund-type`，实现日汇总作业。合并时切回主分支：

```bash
git switch main
git merge --no-ff demo/customer-last-date
git merge --no-ff demo/daily-fund-type
bash scripts/run.sh customer_fund
bash scripts/run.sh daily_fund_type
git log --graph --oneline --all
```

两份目录各自配置 `demo.env`，使用不同的 `DEMO_MYSQL_SCHEMA` 和 `DEMO_MODEL_ROOT`，可以在同一台 Master 上并行提交。

每次运行的 `CODE_VERSION` 由 `scripts/common.sh` 统一判定，共三种形态：

| 工作目录状态 | `CODE_VERSION` | 含义 |
| --- | --- | --- |
| Git 检出且相关文件无改动 | 40 位提交号 | 可复现的提交 |
| Git 检出但有未提交改动 | 12 位提交号 + `-dirty` | 本地调试，不能作为验收依据 |
| 无 `.git`，有 `REVISION` | `REVISION` 前 12 位 + `-` + 内容哈希前 8 位 | 部署包内一致性校验 |
| 无 `.git` 且无 `REVISION` | `snapshot-` + 内容哈希前 12 位 | 源码快照 |

内容哈希覆盖 `app.py`、`spark_project/`、`scripts/` 和 `sql/`。验收时只认不带 `-dirty` 的版本，并可用同一份代码重跑。

在 `demo-main` 可以用 `git diff origin/demo/baseline...origin/demo/customer-last-date` 查看新增指标及对应核验。基础版与合并版共用初始化脚本，最近确认日期字段允许基础版留空。用 `git diff origin/demo/baseline...origin/demo/daily-fund-type` 查看 B 新增的作业和注册项。两条分支的提交记录可分别查阅。

## 6. 如何验证一次优化

用同一份输入和日期范围运行 `daily_fund_type` 的两个策略。`direct` 逐笔关联基金维表后按日及类别统计。`preagg` 先按日期和基金汇总可相加的笔数、金额，再关联基金维表汇总到类别。记录日志中的 `LOOKUP_INPUT_ROWS` 和执行计划。

```bash
DEMO_SHOW_PLAN=1 bash scripts/run.sh daily_fund_type 2025-01-06 2025-01-31 direct
DEMO_SHOW_PLAN=1 bash scripts/run.sh daily_fund_type 2025-01-06 2025-01-31 preagg
# 分别执行两次日志里的 VERIFY_COMMAND，再用实际编号替换占位符
bash scripts/compare_daily.sh DIRECT_RUN_ID PREAGG_RUN_ID
```

固定样本中，进入基金关联的记录预期从 **1,024 行减为 142 行**。比较脚本核对两份结果的日期范围、条数、笔数和金额，末行返回 `PASS`。耗时和 Shuffle 会受实际集群调度及执行计划影响。现场可用 Spark UI 记录这些指标。

## 7. 验收记录

在 148 的 Spark 2.4.3 Standalone、MySQL 8.0、Connector/J 8.0.33 环境用 `scripts/demo_all.sh` 跑通整条链路：环境自检、建库、FAR-Trans 导入、`customer_fund` ETL、ML 训练，两条独立核验均返回 `PASS`。

| 核验项 | 结果 |
| --- | --- |
| 环境自检 | `scripts/doctor.sh` 全项通过 |
| ETL 对账 | `customer_fund` 回写 60 行，独立 SQL 复算 `PASS` |
| 数据版本 | FAR-Trans ZIP SHA-256 `d0d6cfe027732bab45fdcf9af80eff4fefddc789aedae9bdce0d3d6cfd05cfb6`，32,468 客户 / 318 基金 / 29,666 交易 |
| ML 对账 | 模型保存并重载后评分，`verify_fund_ml.sh` 末行 `PASS` |
| 一键流程 | `scripts/demo_all.sh` 末行 `DEMO COMPLETE` |

应用 ID、`run_id` 与逐项指标以现场日志和 `demo_run`、`fund_ml_run` 表为准。

更早一轮在隔离 Spark 3.5.8 / MySQL 8.0.26 / Connector/J 8.4.0 上验证过 Git 分支协作、并行提交和预聚合优化，结论与 2.4.3 一致。

| 核验项 | 结果 |
| --- | --- |
| Git 历史 | 四条分支均可检出，入口和输出字段与各自代码版本一致 |
| 并行提交 | 同一 Master 同时登记两个独立应用，各自写入并核验 |
| 结果对账 | `customer_fund` 60/60，`daily_fund_type` 两种策略各 78/78 |
| 优化比较 | 78 条逐行相同，独立比较为 `PASS`，进入基金关联的记录由 1,024 行减为 142 行 |

那一轮的运行记录：

| 版本 | Spark 应用 ID | MySQL 运行编号 | 结果 |
| --- | --- | --- | --- |
| 基础版 `27bc833` | `app-20261117135239-0002` | `qa_branch_baseline` | 60 行。样例 2 笔、300 元，最近确认日为空。SQL `PASS` |
| 开发分支 A `7ae7af4` | `app-20261117135220-0001` | `qa_branch_customer` | 60 行。样例最近确认日为 2025-01-08。SQL `PASS` |
| 开发分支 B `6fa7dc8` | `app-20261117135219-0000` | `qa_branch_daily` | 78 行。SQL `PASS` |

## 8. 运行记录、清理与打包

| 需求 | 命令 | 说明 |
| --- | --- | --- |
| 查看某次运行 | `bash scripts/report.sh RUN_ID` | 打印登记、结果行数、ML 指标、模型校验和与日志路径 |
| 删除某次运行 | `bash scripts/cleanup_run.sh RUN_ID` | 确认后删除结果与登记行，并清理本地模型目录；日志保留 |
| 复用编号重跑 | `DEMO_RUN_ID=RUN_ID bash scripts/run.sh ...` | 先清理，再以同一编号提交 |
| 打部署包 | `bash scripts/package.sh` | 生成 `dist/spark-project-fund-ml.zip`，含代码、文档、测试和 `data/FAR-Trans.zip` |
| 单元测试 | `python3 -m unittest discover -s tests -v` | 只依赖标准库，不需要 Spark 或 MySQL |

`logs/`、`data/`、`dist/` 与 `demo.env` 都在 `.gitignore` 中：日志和数据按环境生成，部署包由 `package.sh` 现场构建。Git 检出下 `package.sh` 会把当前提交写入包内 `REVISION`；存在未提交改动时拒绝打包，避免交付物与代码版本不一致。FAR-Trans 的来源、许可与固定 SHA-256 记录在 `NOTICE`。

测试覆盖参数与连接校验、FAR-Trans 字段校验和 SQL 字面量转义。Spark 作业本身仍以 `scripts/demo_all.sh` 的端到端验收为准。
