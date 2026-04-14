"""SQLite 数据库初始化脚本"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'career.db')

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(80) UNIQUE NOT NULL,
            email VARCHAR(120) UNIQUE NOT NULL,
            password_hash VARCHAR(200) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS resume (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            filename VARCHAR(200),
            content TEXT,
            parsed_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES user (id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            role VARCHAR(20) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES user (id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_application (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            company VARCHAR(200),
            position VARCHAR(200),
            status VARCHAR(50),
            apply_date DATE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES user (id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name VARCHAR(200) NOT NULL,
            salary VARCHAR(50),
            edu_require VARCHAR(50),
            major_require VARCHAR(200),
            skills TEXT,
            duty TEXT,
            city VARCHAR(50),
            industry VARCHAR(50)
        )
    ''')
    
    jobs_data = [
        ('Python开发工程师','12-20K','本科','计算机相关','Python,Flask,MySQL','后端API开发','北京','互联网'),
        ('Python后端开发','11-18K','本科','计算机相关','Python,FastAPI,Redis','服务端开发','上海','互联网'),
        ('Django开发工程师','10-17K','本科','计算机相关','Django,DRF,PostgreSQL','Web系统开发','深圳','互联网'),
        ('前端开发工程师','11-19K','本科','计算机相关','Vue3,React,JS','页面与组件开发','杭州','互联网'),
        ('Web前端开发','9-15K','本科','计算机相关','Vue,Element Plus','交互实现','广州','互联网'),
        ('全栈开发工程师','15-25K','本科','计算机相关','Vue,Python,MySQL','前后端开发','成都','互联网'),
        ('软件测试工程师','7-12K','本科','计算机相关','功能测试,用例设计','测试与缺陷管理','杭州','互联网'),
        ('自动化测试工程师','10-18K','本科','计算机相关','Python,Selenium','自动化脚本开发','苏州','互联网'),
        ('Java开发工程师','13-22K','本科','计算机相关','SpringBoot,MyBatis','微服务开发','北京','互联网'),
        ('大数据开发工程师','18-30K','本科','计算机相关','Hadoop,Spark','数据仓库开发','上海','大数据'),
        ('数据分析师','9-16K','本科','统计/计算机','Python,SQL,Excel','报表与专题分析','深圳','互联网'),
        ('算法工程师','22-40K','硕士','计算机/数学','机器学习,深度学习','模型训练','北京','AI'),
        ('产品经理','12-22K','本科','不限','需求分析,原型设计','产品规划','上海','互联网'),
        ('产品助理','6-10K','本科','不限','Axure,需求梳理','文档与调研','广州','互联网'),
        ('UI设计师','8-15K','本科','设计类','Figma,PS','界面与图标设计','深圳','互联网'),
        ('运营专员','5-9K','本科','不限','文案,活动策划','日常运营','杭州','互联网'),
        ('新媒体运营','6-11K','本科','不限','短视频,社群','小红书/公众号运营','成都','互联网'),
        ('市场营销专员','6-12K','专科','不限','推广,商务对接','品牌获客','重庆','电商'),
        ('人力资源专员','5-8K','本科','人力相关','招聘,社保','员工管理','南京','企业服务'),
        ('行政文员','4-7K','专科','不限','Office,文档','后勤与接待','武汉','综合企业'),
        ('会计','6-11K','本科','财务类','用友,报税','账务处理','北京','金融'),
        ('出纳','4-7K','专科','财务类','银行对账','资金管理','上海','企业'),
        ('护士','6-10K','专科','护理','护士资格证','临床护理','各地','医疗'),
        ('医生助理','8-15K','本科','临床','病历书写','诊疗辅助','各地','医疗'),
        ('教师','8-15K','本科','师范类','教学设计','课堂教学','各地','教育'),
        ('少儿编程老师','7-12K','本科','计算机/教育','Scratch,Python','编程教学','各地','教育'),
        ('电商运营','7-14K','专科','不限','淘宝,拼多多','店铺管理','杭州','电商'),
        ('物流专员','5-8K','专科','不限','仓储调度','出入库管理','各地','物流'),
        ('采购专员','6-10K','专科','不限','供应商比价','采购执行','各地','制造业'),
        ('行政主管','7-12K','本科','管理类','行政管理','团队统筹','各地','企业')
    ]
    
    cursor.executemany(
        'INSERT INTO jobs (job_name, salary, edu_require, major_require, skills, duty, city, industry) VALUES (?,?,?,?,?,?,?,?)',
        jobs_data
    )
    
    conn.commit()
    conn.close()
    print(f"数据库初始化完成: {DB_PATH}")
    print(f"已导入 {len(jobs_data)} 条岗位数据")

if __name__ == '__main__':
    init_database()