#!/usr/bin/env python3
"""
查询最终字段到物理表的完整溯源链路
"""
from neo4j import GraphDatabase
import csv

class FinalFieldLineageQuery:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query_all_final_fields_lineage(self):
        """查询所有最终字段的溯源"""

        # 查询所有ROOT节点的字段（最终输出）
        query = """
        MATCH (f:Field)
        WHERE f.node_id STARTS WITH 'ROOT_'
        OPTIONAL MATCH (p:PhysicalTable)-[:PROVIDES]->(f)
        RETURN f.name AS 最终字段名,
               f.node_id AS 所属节点,
               p.name AS 物理表,
               CASE WHEN p IS NOT NULL THEN '成功溯源' ELSE '无法溯源' END AS 溯源状态
        ORDER BY 溯源状态 DESC, 最终字段名
        """

        with self.driver.session() as session:
            result = session.run(query)
            records = list(result)

            return records

    def query_statistics(self):
        """查询统计数据"""

        queries = {
            "总字段数": "MATCH (f:Field) RETURN count(*) AS count",
            "有PROVIDES关系的字段数": "MATCH (f:Field)<-[:PROVIDES]-() RETURN count(*) AS count",
            "物理表数": "MATCH (p:PhysicalTable) RETURN count(*) AS count",
            "PROVIDES关系数": "MATCH ()-[r:PROVIDES]->() RETURN count(r) AS count",
            "DERIVES关系数": "MATCH ()-[r:DERIVES]->() RETURN count(r) AS count",
        }

        stats = {}
        with self.driver.session() as session:
            for name, query in queries.items():
                result = session.run(query)
                record = result.single()
                stats[name] = record['count']

        return stats

    def export_to_csv(self, records, output_file):
        """导出到CSV"""

        with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['最终字段名', '所属节点', '物理表', '溯源状态'])

            for record in records:
                writer.writerow([
                    record['最终字段名'],
                    record['所属节点'],
                    record['物理表'] or '',
                    record['溯源状态']
                ])

        print(f"✓ CSV已导出: {output_file}")

def main():
    print("=" * 80)
    print("最终字段溯源查询 - Neo4j")
    print("=" * 80)

    query = FinalFieldLineageQuery()

    # 1. 查询统计数据
    print("\n【统计信息】")
    stats = query.query_statistics()
    for name, count in stats.items():
        print(f"  {name}: {count:,}")

    # 2. 查询所有最终字段溯源
    print("\n正在查询最终字段溯源...")
    records = query.query_all_final_fields_lineage()

    # 3. 统计溯源成功率
    total = len(records)
    traced = sum(1 for r in records if r['溯源状态'] == '成功溯源')
    failed = total - traced
    success_rate = (traced / total * 100) if total > 0 else 0

    print(f"\n【溯源统计】")
    print(f"  最终字段总数: {total:,}")
    print(f"  成功溯源: {traced:,}")
    print(f"  无法溯源: {failed:,}")
    print(f"  溯源成功率: {success_rate:.2f}%")

    # 4. 显示成功溯源的示例（前20个）
    print(f"\n【成功溯源示例（前20个）】")
    traced_count = 0
    for record in records:
        if record['溯源状态'] == '成功溯源':
            traced_count += 1
            if traced_count <= 20:
                print(f"  {traced_count}. {record['最终字段名']}")
                print(f"     -> 物理表: {record['物理表']}")
                print(f"     -> 所属节点: {record['所属节点']}")

    # 5. 显示无法溯源的示例（前20个）
    print(f"\n【无法溯源示例（前20个）】")
    failed_count = 0
    for record in records:
        if record['溯源状态'] == '无法溯源':
            failed_count += 1
            if failed_count <= 20:
                print(f"  {failed_count}. {record['最终字段名']} ({record['所属节点']})")

    # 6. 按物理表分组统计
    print(f"\n【物理表使用统计 TOP 10】")
    table_stats = {}
    for record in records:
        if record['物理表']:
            table = record['物理表']
            table_stats[table] = table_stats.get(table, 0) + 1

    sorted_tables = sorted(table_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    for i, (table, count) in enumerate(sorted_tables, 1):
        print(f"  {i}. {table}: {count}个字段")

    # 7. 导出CSV
    output_file = "/Users/gonghang/Desktop/产品/血缘分析工具/批量分析结果/最终字段溯源结果.csv"
    query.export_to_csv(records, output_file)

    query.close()

    print("\n" + "=" * 80)
    print("✅ 查询完成!")
    print("=" * 80)

    return stats, traced, failed, success_rate

if __name__ == "__main__":
    main()
