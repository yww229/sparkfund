# 基金买入二分类 · Spark 机器学习 Demo

给定一个观察日，使用客户画像、历史基金买入和基金子类，预测该客户在未来 30 天内是否会在数据来源机构买入某只基金。工程沿用现有 `spark-submit`、MySQL 和 Git 分支工作流。交付包含样本构建、逻辑回归、模型保存与重载、测试集评估和独立 SQL 对账。

## 1. 数据和标签

数据来自格拉斯哥大学发布的 **FAR-Trans**，许可为 CC BY 4.0。源版本由 ZIP 的 SHA-256 固定。导入脚本保留 32,468 条客户画像快照、318 条基金资料快照和 29,666 条基金买卖记录。基金资料含 315 个唯一 ISIN。 [官方数据发布页](https://researchdata.gla.ac.uk/1658/) · [原始字段说明](https://researchdata.gla.ac.uk/1658/2/README.txt)

| 样本口径 | 规则 |
| --- | --- |
| 一行样本 | 一位客户 × 一只基金 × 一个观察日 `T` |
| 候选范围 | 用最早观察日及之前的有效买入选定 10 只基金。各观察日以此前 365 天内有基金交易的客户作为候选，按历史行为确定客户上限 |
| 画像和属性 | 用 `T` 及以前的最近客户和基金快照，保留客户类型、风险等级、投资能力和基金子类 |
| 交易特征 | 过去 90 天买入该基金的次数、买入任意基金的次数。窗口为 `(T−90 天, T]` |
| 正类 `1` | 在 `(T, T+30 天]` 内记录到该客户买入该基金 |
| 负类 `0` | 上述窗口内记录中没有该组合的买入 |

原始样本的正样本占比极低，逻辑回归会退化成常数模型。构建时按观察日做确定性负采样：每个正例保留 20 个负例（`DEMO_NEGATIVES_PER_POSITIVE`，默认 20），训练、验证、测试三个切分各自按此比例采样。正负样本比不是随机丢失，同样的输入会得到同样的样本集，指标按采样后的测试集口径解读。采样窗口、负例比例和候选上限会拼成 `sample_signature` 写入 `fund_ml_run`，便于比较两次运行是否使用同一口径。

发布方在价格起始日补录初始持仓买入，在价格结束日补录部分卖出。样本构建保守排除起始日买入和结束日卖出，也会排除同一天的真实交易。一个客户可以在不同观察日进入不同数据集，模拟较早历史训练后对后续时间做批量预测。 [原论文数据清理说明](https://arxiv.org/html/2407.08692v1#S3.SS3)

| 数据集 | 观察日 | 用途 |
| --- | --- | --- |
| 训练 | 2020-06-30、2020-09-30、2020-12-31、2021-03-31、2021-06-30、2021-09-30 | 拟合分类特征编码和逻辑回归 |
| 验证 | 2021-12-31、2022-03-31 | 从预设阈值中选择 F1 最大的阈值 |
| 测试 | 2022-06-30、2022-09-30 | 使用已保存并重载的模型评分，报告 PR-AUC、ROC-AUC、精确率、召回率和混淆矩阵 |

测试集同时记录正样本占比，作为随机排序的 PR-AUC 参考。验收核对时间切分、数据版本、模型重载和结果对账，指标以实际测试输出为准。

## 2. 与现有工程怎么接

```text
FAR-Trans ZIP → MySQL 源表 → samples.py 生成特征与标签
             → model.py 拟合、选阈值、保存、重载和评分
             → job.py 回写预测、指标和运行登记
             → verify_fund_ml.sh 在 MySQL 独立复算
```

| 位置 | 开发内容 |
| --- | --- |
| `spark_project/fund_ml/import_far_trans.py` | 校验发布版本，解析源文件并生成入库事务 |
| `spark_project/fund_ml/samples.py` | 构建时间对齐的样本、候选组合和未来 30 天标签 |
| `spark_project/fund_ml/model.py` | 组装特征，逻辑回归拟合，验证集选阈值，原生 Spark 模型保存和重载 |
| `spark_project/fund_ml/job.py` | 复用工程 JDBC 组件，回写并登记 `run_id`、数据版本、代码版本、Spark 应用 ID 和模型路径 |
| `sql/setup_ml.sql`、`sql/verify_fund_ml.sql` | 初始化源表和模型结果表，独立复算标签和分类指标 |

样本与训练模块的改动可以分别放在 `demo/ml-samples`、`demo/ml-training` 分支，合并版本在 `main`。PyCharm、Trae、WorkBuddy、VS Code 都使用同一套入口。两个终端连接同一个 Spark Master，可用各自的工作目录和模型输出路径独立提交。 [Spark 2.4.3 逻辑回归](https://archive.apache.org/dist/spark/docs/2.4.3/ml-classification-regression.html#logistic-regression)

## 3. 部署与运行

在公司网络 SSH 登录 148。解压部署包后进入项目目录，本机为 `/data/spark-project-fund-mlnew`。从 `config/demo.env.example` 复制 `demo.env`，填入 Spark Master、Connector/J 和 MySQL 连接信息。增加 `DEMO_MODEL_ROOT` 指向 **Driver 与 Executor 均可见且可写的绝对存储路径**，例如团队 HDFS 的工作目录，或所有进程都挂载到同一路径的共享目录。训练会在其下创建 `run_id` 子目录。 [Spark 2.4.3 MLlib 依赖](https://archive.apache.org/dist/spark/docs/2.4.3/ml-guide.html#dependencies)

Python 侧按 [README 4.1](README.md) 使用源码编译的 `/data/spark-python37/bin/python3`，并把 `PYTHONPATH` 指向 NumPy 所在的 `py37-site`。`run.sh` 会把该 `PYTHONPATH` 透传给 Executor；只配驱动端时，Executor 会在 `pyspark.ml.linalg` 上报 `No module named 'numpy'`。

```bash
source demo.env
bash scripts/setup_ml_mysql.sh
bash scripts/load_far_trans.sh data/FAR-Trans.zip
bash scripts/run.sh fund_purchase_lr
```

初始化和导入用测试库配置里的建库账号，需要目标库的 `CREATE`、`SELECT`、`INSERT`、`UPDATE`。运行账号对 `far_customer`、`far_asset`、`far_trade`、`far_dataset` 需要 `SELECT`，对 `fund_ml_prediction`、`fund_ml_metric` 和 `fund_ml_run` 需要 `SELECT`、`INSERT`。真实部署的 `data/` 只放这一个官方 ZIP，源码仓库不跟踪它；`load_far_trans.sh` 直接流式解压导入，不需要预先拆分。ZIP 的 SHA-256 是 `d0d6cfe027732bab45fdcf9af80eff4fefddc789aedae9bdce0d3d6cfd05cfb6`。从 Git 克隆代码时，可以用部署包内的同一份 ZIP，或省略路径由脚本从发布站下载。密码按提示输入。

执行成功后，从日志复制 `VERIFY_COMMAND=` 后面的整条命令。MySQL 从源交易复算测试集标签，再根据已保存的分数和阈值复算分类、混淆矩阵及阈值指标。末行 `PASS` 表示独立对账通过。

```bash
bash scripts/verify_fund_ml.sh RUN_ID
```

运行结果保存在 `fund_ml_run`、`fund_ml_metric` 与 `fund_ml_prediction`。`fund_ml_run` 同时记录数据版本、代码版本、Spark 应用 ID、样本签名、日志路径，以及模型目录的 SHA-256 与字节数（本地目录才有，HDFS/S3 等远程存储留空）。前两张表便于查看某次运行的来源、模型位置和指标，预测表用于逐条抽查。模型文件按运行编号保存，可在相同版本的 Spark 环境中加载 `PipelineModel`。

```bash
bash scripts/report.sh RUN_ID      # 登记、指标、模型校验和与日志路径
bash scripts/cleanup_run.sh RUN_ID # 删除本次结果与模型目录，日志保留
```

清理后可用 `DEMO_RUN_ID=RUN_ID bash scripts/run.sh fund_purchase_lr` 以同一编号重跑。

## 4. 验收记录

在 148 的 Spark 2.4.3 Standalone、MySQL 8.0、Connector/J 8.0.33 环境跑通入库、训练、模型保存与重载、回写和独立复核：`setup_ml_mysql.sh` → `load_far_trans.sh` → `run.sh fund_purchase_lr` → `verify_fund_ml.sh`，末行 `PASS`。

| 数据集 | 行数 | 正样本 |
| --- | ---: | ---: |
| 训练 | 以 `ML_SPLIT train` 日志为准 | 同左 |
| 验证 | 以 `ML_SPLIT validation` 日志为准 | 同左 |
| 测试 | 189 | 9 |

上表是本轮交付验收时的记录。测试集按 1:20 负采样后，验证集选出的阈值为 0.10，PR-AUC 约 0.771，F1 约 0.583。训练集和验证集的行数由观察日候选客户与负采样比例共同决定，不写固定值。

`verify_fund_ml.sql` 不依赖具体日期或固定指标数：它从 `fund_ml_run` 取本次 `test_rows` 和阈值，在 MySQL 里逐条复算标签、混淆矩阵和全部指标，要求每条预测都可解释、每条指标都与结果一致且指标数不少于 12。每次运行的准确数字以控制台 `ML_SPLIT`、`ML_THRESHOLD`、`ML_METRIC` 输出和 `fund_ml_metric` 表为准。

更早一版在隔离 Spark 3.5.8 上验过未采样的 60,000 行测试集：PR-AUC 0.01764，阈值 0.01，召回率 0%。该口径没有负采样，指标与当前 1:20 口径不可直接比较，只作历史记录。
