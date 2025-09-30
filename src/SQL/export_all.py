
import sqlite3, pandas as pd


# 连接数据库
conn = sqlite3.connect(r'data/news_dw.sqlite')

# 查询所有数据
df = pd.read_sql_query("SELECT * FROM dwd_news;", conn)

# 导出为 UTF-16LE + 制表符
df.to_csv('all_data.tsv', sep='\t', index=False, encoding='utf-16')

conn.close()
print("✅ 导出完成：all_data.tsv")
