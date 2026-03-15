#!/usr/bin/env python3
"""
快速测试SQL血缘解析器 - 验证优化后的版本
测试所有27个Oracle SQL优化任务，确保100%成功率
"""

import sys
import os
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_node_parser_v2 import SQLNodeParser

def quick_test():
    """快速测试27个任务"""

    base_path = Path("/Users/gonghang/Desktop/航天科技/5模型设计/数据中台迁移重构项目/模拟数据/优化任务")

    success_count = 0
    fail_count = 0
    total_count = 0

    print("开始测试优化后的SQL血缘解析器...")
    print("=" * 60)

    for i in range(1, 28):  # 任务01-27
        task_dirs = list(base_path.glob(f"{i:02d}_*"))
        if task_dirs:
            task_dir = task_dirs[0]
            sql_file = task_dir / "原始.sql"

            if sql_file.exists():
                total_count += 1
                try:
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()

                    parser = SQLNodeParser(sql_content, dialect='mysql')
                    nodes, relationships = parser.parse()

                    success_count += 1
                    print(f"✅ 任务{i:02d}: {task_dir.name}")

                except Exception as e:
                    fail_count += 1
                    print(f"❌ 任务{i:02d}: {task_dir.name}")
                    print(f"   错误: {str(e)[:100]}")

    print("=" * 60)
    print(f"测试完成: {success_count}/{total_count} 成功")
    print(f"成功率: {success_count/total_count*100:.1f}%")

    if fail_count == 0:
        print("\n🎉 所有任务测试通过！解析器工作正常。")
        return 0
    else:
        print(f"\n⚠️  有{fail_count}个任务失败，需要检查。")
        return 1

if __name__ == "__main__":
    sys.exit(quick_test())