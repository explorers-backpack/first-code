import pymysql

conn = pymysql.connect(
    host='localhost',
    user='root',
    password='2549966637',
    database='career',
    charset='utf8mb4'
)
cursor = conn.cursor()

# 建表
tables = [
    """CREATE TABLE IF NOT EXISTS `user` (
      id INT PRIMARY KEY AUTO_INCREMENT,
      username VARCHAR(80) UNIQUE NOT NULL,
      email VARCHAR(120) UNIQUE NOT NULL,
      password_hash VARCHAR(200) NOT NULL,
      role VARCHAR(20) DEFAULT 'user',
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS resume (
      id INT PRIMARY KEY AUTO_INCREMENT,
      user_id INT NOT NULL,
      filename VARCHAR(200),
      content TEXT,
      parsed_data JSON,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS chat_history (
      id INT PRIMARY KEY AUTO_INCREMENT,
      user_id INT NOT NULL,
      role VARCHAR(20) NOT NULL,
      content TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS user_session (
      id INT PRIMARY KEY AUTO_INCREMENT,
      user_id INT NOT NULL,
      token VARCHAR(64) UNIQUE NOT NULL,
      email VARCHAR(120) NOT NULL,
      role VARCHAR(20) NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_token (token)
    )""",
    """CREATE TABLE IF NOT EXISTS user_login_log (
      id INT PRIMARY KEY AUTO_INCREMENT,
      email VARCHAR(120) NOT NULL,
      role VARCHAR(20) NOT NULL,
      last_login VARCHAR(32) NOT NULL,
      search_count INT DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS jobs (
      id INT PRIMARY KEY AUTO_INCREMENT,
      job_name VARCHAR(200) NOT NULL,
      salary VARCHAR(50),
      edu_require VARCHAR(50),
      major_require VARCHAR(200),
      skills TEXT,
      duty TEXT,
      city VARCHAR(50),
      industry VARCHAR(50)
    )""",
]

for sql in tables:
    cursor.execute(sql)

conn.commit()

# 初始化 jobs 数据
cursor.execute('SELECT COUNT(*) FROM jobs')
if cursor.fetchone()[0] == 0:
    jobs = [
        ('Python开发工程师','12-20K','本科','计算机相关','Python,Flask,MySQL','后端API开发','北京','互联网'),
        ('Python后端开发','11-18K','本科','计算机相关','Python,FastAPI,Redis','服务端开发','上海','互联网'),
        ('Django开发工程师','10-17K','本科','计算机相关','Django,DRF,PostgreSQL','Web系统开发','深圳','互联网'),
        ('前端开发工程师','11-19K','本科','计算机相关','Vue3,React,JS','页面与组件开发','杭州','互联网'),
        ('Web前端开发','9-15K','本科','计算机相关','Vue,Element Plus','交互实现','广州','互联网'),
        ('全栈开发工程师','15-25K','本科','计算机相关','Vue,Python,MySQL','前后端开发','成都','互联网'),
        ('自动化测试工程师','10-18K','本科','计算机相关','Python,Selenium','自动化脚本开发','苏州','互联网'),
        ('Java开发工程师','13-22K','本科','计算机相关','SpringBoot,MyBatis','微服务开发','北京','互联网'),
        ('大数据开发工程师','18-30K','本科','计算机相关','Hadoop,Spark','数据仓库开发','上海','大数据'),
        ('数据分析师','9-16K','本科','统计/计算机','Python,SQL,Excel','报表与专题分析','深圳','互联网'),
        ('算法工程师','22-40K','硕士','计算机/数学','机器学习,深度学习','模型训练','北京','AI'),
    ]
    cursor.executemany(
        'INSERT INTO jobs (job_name,salary,edu_require,major_require,skills,duty,city,industry) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
        jobs
    )
    conn.commit()
    print(f'Seeded {len(jobs)} jobs')

cursor.execute('SHOW TABLES')
print('Tables:', [r[0] for r in cursor.fetchall()])
conn.close()
print('Done.')
