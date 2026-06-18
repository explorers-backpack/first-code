import pymysql
import os

conn = pymysql.connect(
    host=os.getenv('DB_HOST', 'localhost'),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PWD', ''),
    database='career',
    charset='utf8mb4'
)
cursor = conn.cursor()
cursor.execute("UPDATE user SET role='admin' WHERE username='admin'")
conn.commit()
print('Updated admin role to admin')
cursor.execute('SELECT id, username, email, role FROM user')
for row in cursor:
    print(row)
conn.close()
