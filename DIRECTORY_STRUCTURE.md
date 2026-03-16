# 目录结构说明

## 项目结构

```
血缘分析工具/
├── data/                    # 数据目录
│   └── sql/                 # 原始SQL文件
│       ├── 01_xxx/原始.sql
│       ├── 02_xxx/原始.sql
│       └── ...
│
├── outputs/                 # 输出结果目录
│   └── lineage/            # 血缘分析结果
│       ├── 汇总报告.csv
│       ├── 01_xxx/
│       │   ├── 字段血缘.csv
│       │   └── 表关联关系.csv
│       └── ...
│
├── src/                     # 源代码
│   ├── parsers/            # SQL解析器
│   ├── analyzers/          # 分析器
│   ├── exporters/          # 导出器
│   └── metadata/           # 元数据管理
│
├── metadata/                # 元数据文件
│   ├── 大数据ods的实例库表字段.csv
│   └── 大数据dw和dm的实例库表字段.csv
│
├── main.py                  # 主入口
├── batch_analyze.py        # 批量分析工具
├── trace_field_lineage.py  # 字段血缘追踪
└── CLAUDE.md               # 项目文档
```

## 使用方法

### 分析单个SQL文件
```bash
python main.py data/sql/01_xxx/原始.sql
```

### 批量分析所有SQL文件
```bash
python batch_analyze.py
```

### 指定SQL目录和输出目录
```bash
python batch_analyze.py --sql-dir data/sql --output-dir outputs/lineage
```

### 查看分析结果
```bash
# 汇总报告
cat outputs/lineage/汇总报告.csv

# 某个任务的血缘分析
cat outputs/lineage/01_xxx/字段血缘.csv
```

## 默认配置

batch_analyze.py 的默认配置：
- `--sql-dir`: `data/sql` (SQL文件目录)
- `--output-dir`: `outputs/lineage` (输出目录)
- `--metadata`: `metadata/大数据ods的实例库表字段.csv metadata/大数据dw和dm的实例库表字段.csv`
