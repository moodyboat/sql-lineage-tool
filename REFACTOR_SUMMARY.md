# 目录重构总结

## 重构完成时间
2026年3月16日

## 重构内容

### 1. 目录结构优化

**重构前**：
```
血缘分析工具/
├── outputs/优化后/           # SQL文件位置（不明确）
├── 血缘分析结果/             # 分析结果（根目录混乱）
└── batch_analyze.py         # 配置路径不匹配
```

**重构后**：
```
血缘分析工具/
├── data/                    # 数据目录（清晰）
│   └── sql/                 # 原始SQL文件
│       ├── 01_xxx/原始.sql
│       └── ...
│
├── outputs/                 # 输出结果（统一管理）
│   └── lineage/            # 血缘分析结果
│       ├── 汇总报告.csv
│       ├── 01_xxx/
│       │   ├── 字段血缘.csv
│       │   └── 表关联关系.csv
│       └── ...
│
├── src/                     # 源代码
├── metadata/                # 元数据
├── main.py                  # 主入口
└── batch_analyze.py        # 批量分析（配置已修正）
```

### 2. 配置文件更新

**batch_analyze.py**：
- `--sql-dir`: `'优化后'` → `'data/sql'`
- `--output-dir`: `'血缘分析结果'` → `'outputs/lineage'`

### 3. 文档更新

- ✅ `README.md` - 更新项目结构和示例路径
- ✅ `DIRECTORY_STRUCTURE.md` - 新增目录结构详细说明
- ✅ `REFACTOR_SUMMARY.md` - 本文件

### 4. 优势

1. **清晰的职责分离**：
   - `data/` - 存放原始数据
   - `outputs/` - 存放分析结果
   - `src/` - 存放源代码

2. **更好的可维护性**：
   - 配置路径与实际目录一致
   - 避免路径混淆

3. **更好的扩展性**：
   - 可以在 `data/` 下添加其他数据类型
   - 可以在 `outputs/` 下添加其他分析结果

## 使用方法

### 批量分析（使用默认配置）
```bash
python batch_analyze.py
```

### 查看结果
```bash
# 汇总报告
cat outputs/lineage/汇总报告.csv

# 某个任务的血缘
cat outputs/lineage/01_xxx/字段血缘.csv
```

### 分析单个SQL
```bash
python main.py data/sql/01_xxx/原始.sql
```

## 验证结果

- ✅ SQL文件已移动到 `data/sql/`
- ✅ 分析结果已移动到 `outputs/lineage/`
- ✅ 配置文件已更新
- ✅ 批量分析测试通过
- ✅ 文档已同步更新

## 注意事项

如果其他脚本或工具引用了旧路径，请相应更新：
- `outputs/优化后/` → `data/sql/`
- `血缘分析结果/` → `outputs/lineage/`
