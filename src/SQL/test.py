import sqlite3, pandas as pd

conn = sqlite3.connect(r'data/news_dw.sqlite')
df = pd.read_sql_query("""
SELECT title,category,region,platform_type,llm_confidence,llm_reason
FROM dwd_news
WHERE week_tag='2025-W37' AND valid=1
ORDER BY published_at DESC
LIMIT 20;
""", conn)
conn.close()

# Excel 100% 不乱码：UTF-16LE + 制表符
df.to_csv('check_thisweek.tsv', sep='\t', index=False, encoding='utf-16')
