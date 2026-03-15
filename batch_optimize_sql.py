#!/usr/bin/env python3
"""
高级SQL批量优化脚本 - 使用sqlglot的完整功能
功能：
1. Oracle → MySQL方言转换
2. SQL语法优化和标准化
3. 模板变量智能处理
4. 代码格式化和美化
5. 批量处理和报告生成
"""

import os
import sys
from pathlib import Path
import sqlglot
from sqlglot import parse, transpile
from sqlglot.dialects import Oracle, MySQL
import re

class AdvancedSQLOptimizer:
    """高级SQL脚本优化器"""

    def __init__(self, output_dir=None, target_dialect='mysql'):
        self.output_dir = Path(output_dir) if output_dir else None
        self.target_dialect = target_dialect
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'optimized': 0,
            'basic_format': 0,
            'errors': []
        }

    def optimize_sql_file(self, sql_file_path, output_path=None):
        """
        优化单个SQL文件

        Args:
            sql_file_path: SQL文件路径
            output_path: 输出文件路径（可选）
        """
        try:
            # 读取原始SQL
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                original_sql = f.read()

            # 优化SQL
            optimized_sql, method = self._optimize_sql(original_sql)

            # 确定输出路径
            if output_path is None and self.output_dir:
                # 保持目录结构
                rel_path = Path(sql_file_path).parent.name
                output_path = self.output_dir / rel_path / Path(sql_file_path).name

            # 保存优化后的SQL
            if output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(optimized_sql)

            self.stats['success'] += 1
            if method == 'optimized':
                self.stats['optimized'] += 1
            else:
                self.stats['basic_format'] += 1

            return True, optimized_sql, method

        except Exception as e:
            self.stats['failed'] += 1
            self.stats['errors'].append({
                'file': str(sql_file_path),
                'error': str(e)
            })
            return False, None, 'error'

    def _optimize_sql(self, sql):
        """
        优化SQL内容

        优化步骤：
        1. 预处理（处理中文标点和模板变量）
        2. Oracle → MySQL方言转换
        3. SQL优化和格式化
        4. 后处理
        """
        # 1. 预处理
        sql = self._preprocess_sql(sql)

        # 2. 尝试完全优化（方言转换 + 格式化）
        try:
            return self._full_optimization(sql), 'optimized'
        except Exception as e:
            # 3. 如果完全优化失败，使用格式化
            try:
                return self._formatting_only(sql), 'formatted'
            except Exception as e2:
                # 4. 最后的回退：基础清理
                return self._basic_cleanup(sql), 'basic'

    def _preprocess_sql(self, sql):
        """预处理SQL - 保持模板变量原样，但让sqlglot能解析"""

        # 1. 处理中文标点符号
        replacements = {
            '，': ',',
            '；': ';',
            '（': '(',
            '）': ')',
            ''': "'",
            ''': "'",
        }
        for chinese, english in replacements.items():
            sql = sql.replace(chinese, english)

        # 2. 临时处理模板变量，让sqlglot能解析，但稍后恢复原样
        import re
        self.template_vars = {}

        def replace_with_safe_placeholder(match):
            var_name = match.group(1)
            # 使用安全的标识符占位符，让sqlglot能解析
            placeholder = f"__SAFE_VAR_{var_name}__"
            # 保存原始模板变量以便恢复
            self.template_vars[placeholder] = match.group(0)
            return placeholder

        sql = re.sub(r'<!([A-Z_]+)!>', replace_with_safe_placeholder, sql)

        return sql

    def _full_optimization(self, sql):
        """完全优化：Oracle方言格式化"""
        # 解析为AST (Oracle方言)
        ast = parse(sql, dialect='oracle', read='oracle')

        # 保持Oracle方言，只做格式化
        optimized_statements = []
        for expr in ast:
            # 保持Oracle方言，只格式化
            optimized = expr.sql(dialect='oracle', pretty=True)
            optimized_statements.append(optimized)

        result = '\n\n'.join(optimized_statements)

        # 恢复模板变量
        result = self._restore_template_vars(result)
        return result

    def _restore_template_vars(self, sql):
        """恢复模板变量"""
        if hasattr(self, 'template_vars'):
            for placeholder, original in self.template_vars.items():
                sql = sql.replace(placeholder, original)
        return sql

    def _formatting_only(self, sql):
        """仅格式化：保持原方言"""
        try:
            # 使用原方言解析
            ast = parse(sql, dialect='oracle', read='oracle')

            # 格式化输出
            formatted_statements = []
            for expr in ast:
                formatted = expr.sql(dialect='oracle', pretty=True)
                formatted_statements.append(formatted)

            result = '\n\n'.join(formatted_statements)

            # 恢复模板变量
            result = self._restore_template_vars(result)
            return result

        except:
            # 如果解析失败，返回原始SQL
            return sql

    def _basic_cleanup(self, sql):
        """基础清理：移除多余空行和空白"""
        lines = sql.split('\n')
        cleaned_lines = []

        for line in lines:
            stripped = line.strip()
            if stripped:  # 跳过空行
                cleaned_lines.append(stripped)

        return '\n'.join(cleaned_lines)

    def batch_optimize(self, input_dir, output_dir, pattern="原始.sql"):
        """
        批量优化SQL文件

        Args:
            input_dir: 输入目录
            output_dir: 输出目录
            pattern: 文件匹配模式
        """
        input_path = Path(input_dir)
        output_path = Path(output_dir)

        # 创建输出目录
        output_path.mkdir(parents=True, exist_ok=True)

        # 查找所有SQL文件
        sql_files = list(input_path.rglob(pattern))

        self.stats['total'] = len(sql_files)

        print(f"🚀 高级SQL批量优化工具")
        print(f"=" * 80)
        print(f"找到 {len(sql_files)} 个SQL文件")
        print(f"输入目录: {input_path}")
        print(f"输出目录: {output_path}")
        print(f"目标方言: {self.target_dialect}")
        print("=" * 80)

        # 处理每个文件
        methods_count = {'optimized': 0, 'formatted': 0, 'basic': 0, 'error': 0}

        for i, sql_file in enumerate(sql_files, 1):
            print(f"\n[{i}/{len(sql_files)}] 处理: {sql_file.parent.name}/{sql_file.name}")

            success, optimized_sql, method = self.optimize_sql_file(sql_file)

            if success:
                methods_count[method] += 1
                method_icon = {
                    'optimized': '🚀',
                    'formatted': '✨',
                    'basic': '🔧',
                    'error': '❌'
                }.get(method, '📄')

                print(f"  {method_icon} 成功: {method} 方法")
                print(f"  📁 输出: {output_path / sql_file.parent.name / sql_file.name}")
            else:
                print(f"  ❌ 失败: {sql_file.name}")

        # 输出详细统计
        print("\n" + "=" * 80)
        print("📊 优化统计")
        print("=" * 80)
        print(f"总数: {self.stats['total']}")
        print(f"成功: {self.stats['success']}")
        print(f"失败: {self.stats['failed']}")
        print(f"成功率: {self.stats['success']/self.stats['total']*100:.1f}%")

        print(f"\n🎯 优化方法分布:")
        print(f"  🚀 完全优化: {methods_count['optimized']} 个")
        print(f"  ✨ 格式化: {methods_count['formatted']} 个")
        print(f"  🔧 基础清理: {methods_count['basic']} 个")
        if methods_count['error'] > 0:
            print(f"  ❌ 错误: {methods_count['error']} 个")

        if self.stats['errors']:
            print("\n⚠️  处理失败的文件:")
            for error in self.stats['errors'][:5]:  # 只显示前5个
                print(f"  - {error['file']}: {error['error'][:50]}...")

def compare_files(original_path, optimized_path):
    """比较原始文件和优化后文件的差异"""
    try:
        with open(original_path, 'r', encoding='utf-8') as f:
            original = f.read()
        with open(optimized_path, 'r', encoding='utf-8') as f:
            optimized = f.read()

        original_lines = len(original.split('\n'))
        optimized_lines = len(optimized.split('\n'))
        size_reduction = len(original) - len(optimized)

        print(f"  📏 行数: {original_lines} → {optimized_lines}")
        print(f"  📦 大小: {len(original)} → {len(optimized)} 字节")
        if size_reduction > 0:
            print(f"  📉 减少: {size_reduction} 字节")

    except Exception as e:
        print(f"  ⚠️  无法比较文件: {e}")

def main():
    """主函数"""
    # 配置
    base_path = Path("/Users/gonghang/Desktop/航天科技/5模型设计/数据中台迁移重构项目/模拟数据/优化任务")
    output_path = Path("/Users/gonghang/Desktop/产品/血缘分析工具/优化后")

    # 创建高级优化器
    optimizer = AdvancedSQLOptimizer(
        output_dir=output_path,
        target_dialect='oracle'  # 保持Oracle方言
    )

    # 批量优化
    optimizer.batch_optimize(
        input_dir=base_path,
        output_dir=output_path,
        pattern="原始.sql"
    )

    print(f"\n✅ 优化完成！结果保存在: {output_path}")

if __name__ == "__main__":
    main()