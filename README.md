# Legal Data Lakehouse

一个端到端的Legal数据Lakehouse demo项目，在真实AWS + Databricks环境（Unity Catalog）上完整验证，模拟State Street "Data Engineer, Databricks — Legal & Security" JD要求的核心技术能力。

## 架构

S3（原始JSON） → Bronze（Delta，Unity Catalog） → Silver（清洗+去重+DQ校验） → Gold（业务聚合表）

## 技术栈

- Databricks（Unity Catalog、Delta Lake、PySpark）
- AWS（S3、IAM、Storage Credential）
- 数据质量框架（自定义校验模块，本地pytest验证 + 真实Unity Catalog环境二次验证）

## 项目结构

- `data_generation/` — 合成数据生成器（故意注入重复记录、格式不统一、孤儿外键，用于验证Silver层清洗逻辑）
- `dq/` — 可复用的数据质量校验模块（null检查、referential integrity、去重检测）
- `notebooks/` — Bronze/Silver/Gold三层Databricks notebook（真实运行、验证通过）
- `local_run/` — 本地验证脚本

## 真实运行结果

315条合同（含15条故意重复） → 清洗去重后300条
801条义务（含1条故意造的孤儿外键） → DQ校验后800条有效 + 1条精准隔离

## 踩过的坑

开发过程中真实排查并解决了4个技术问题（Unity Catalog存储位置解析边界情况、Spark Connect streaming状态异常、Unity Catalog对legacy API的限制、ANSI模式下日期解析行为变化），详见项目内部文档。