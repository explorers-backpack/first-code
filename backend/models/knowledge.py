# -*- coding: utf-8 -*-
"""AI 面试知识库 ORM 模型（2 张表）。

本阶段范围（重要）
------------------
**只建立基础数据结构**：文档主表 + 切片从表。刻意**不实现**：

- 文档解析（PDF / Word / Markdown → 纯文本）
- Embedding（向量化）
- 向量数据库（chromadb / faiss / milvus …）
- Retriever（``services/knowledge_retriever.py`` 的真实实现）
- InterviewAgent / InterviewCore 的任何修改

也就是说，本模块只回答「知识存在哪儿、长什么样」，
不回答「怎么把文档变成知识」「怎么把问题变成向量」「怎么检索」。

为什么是两张表
--------------
::

    KnowledgeDocument   一次导入 = 一条（存原文全文）
          │ 1:N
          ▼
    KnowledgeChunk      检索的最小单位（document_id 指回所属文档）

检索时命中的是**切片**而不是整篇文档（整篇塞进 Prompt 会超长且噪声大），
但溯源、展示、删除、重新切分都要回到**文档**粒度，所以两层分开存。

四类知识（``category``）
------------------------
==================  ==================  ====================================
取值                中文                典型来源
==================  ==================  ====================================
``job``             岗位知识            岗位 JD / 能力模型 / 职级要求
``technical``       技术知识            技术手册 / 官方文档 / 面试题库
``company``         公司知识            公司介绍 / 业务线 / 面试风格
``project``         项目经验知识        真实项目复盘 / 案例库
==================  ==================  ====================================

.. warning::
   **同名不同物**：``models.KnowledgeChunk``（本模块，**数据库行**）与
   ``services.knowledge_retriever.KnowledgeChunk``（**内存值对象**，
   ``@dataclass(frozen=True)``）是两个不同的类：

   - 本模块的 = ORM 行：有 ``id`` / ``document_id``，可被 SQLAlchemy 查询
   - 检索模块的 = 检索结果载体：只有 ``content`` / ``source`` / ``metadata``，
     **零依赖、不 import 数据库**

   两者**刻意不互相 import**。检索模块要保持「零第三方依赖、可脱离 DB 单测」，
   因此**不得在检索模块里加 ``from_row`` 之类的方法**——那会立刻破坏该性质，
   并被 ``tests/test_knowledge_retriever.py`` 的 AST / 子进程守卫拦下。
   将来的真实 Retriever 负责**在它自己的模块里**完成「ORM 行 → 值对象」转换。

字段口径
--------
- ``source`` 是**自由文本来源标识**（如 ``manual://handbook/redis``、
  ``job:123``、``company:字节跳动``）。本阶段**不建** ``job_id`` /
  ``company_id`` 这类强外键——四类知识的归属维度差异太大，
  先用 ``source`` + 切片 ``metadata`` 承载；等真实导入流程定下来，
  再决定是否把高频过滤维度提升为独立列（届时用 ``utils/schema_sync`` 补列）。
- ``KnowledgeChunk.metadata`` 是 JSON 扩展点，放 ``topic`` / ``difficulty`` /
  ``kind`` / ``job_id`` 等**检索期需要过滤或加权**的信息。
- ``KnowledgeChunk`` 另预留三列承载**向量**：``embedding``（JSON，可空）、
  ``embedding_model``（VARCHAR(100)，非空默认空串）、``embedding_dim``（INTEGER，可空）。
  本阶段**只建结构、不写入**——写入由 ``services/embedding_service.py`` 的下一步负责。
  向量**不放进 ``metadata``**：``metadata`` 是检索期要随切片一路传下去的轻量元信息，
  把上千个浮点数塞进去会让它变成负载。
  口径：``embedding IS NULL`` = 尚未向量化；``embedding_model`` 记录「哪次编码产生的」，
  换模型后据此筛出旧向量重算（不同模型的向量**不可比**）。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)

from database import Base

# ============================================================
# 知识分类（供 service / schema / 导入脚本共用，避免散落魔法字符串）
# ============================================================
KNOWLEDGE_CATEGORY_JOB = "job"
KNOWLEDGE_CATEGORY_TECHNICAL = "technical"
KNOWLEDGE_CATEGORY_COMPANY = "company"
KNOWLEDGE_CATEGORY_PROJECT = "project"

#: 四类知识的合法取值（顺序即展示顺序）。
#: 合法性由 service 层校验（与 ``DIFFICULTIES`` / ``INTERVIEW_TYPES``
#: 的处理方式一致——**不用数据库 ENUM**，避免改枚举要改表结构）。
KNOWLEDGE_CATEGORIES = (
    KNOWLEDGE_CATEGORY_JOB,
    KNOWLEDGE_CATEGORY_TECHNICAL,
    KNOWLEDGE_CATEGORY_COMPANY,
    KNOWLEDGE_CATEGORY_PROJECT,
)

#: 中文名（API / 前端展示用，与 ``KNOWLEDGE_CATEGORIES`` 一一对应）。
KNOWLEDGE_CATEGORY_LABELS = {
    KNOWLEDGE_CATEGORY_JOB: "岗位知识",
    KNOWLEDGE_CATEGORY_TECHNICAL: "技术知识",
    KNOWLEDGE_CATEGORY_COMPANY: "公司知识",
    KNOWLEDGE_CATEGORY_PROJECT: "项目经验知识",
}


class KnowledgeDocument(Base):
    """知识文档主表：一次导入 = 一条记录。

    存**原文全文**（``content``）而不是只存切片——切片是检索用的**派生数据**，
    原文才是可追溯、可重新切分、可整体删除的权威来源。
    """

    __tablename__ = "knowledge_document"

    id = Column(Integer, primary_key=True, autoincrement=True)

    title = Column(String(300), nullable=False)
    content = Column(Text, nullable=False)

    # 四类知识之一，取值见 KNOWLEDGE_CATEGORIES
    category = Column(String(30), nullable=False, index=True)

    # 来源标识（自由文本）：面试场景下「这条知识从哪来」必须可解释、可申诉
    # server_default 与 default 口径一致（同 InterviewSession.interview_mode 的写法）：
    # 让「既有库 ALTER 补列」与「新建表」两条路径落出同样的默认值，
    # 也保证 utils/schema_sync.py 里登记的 DDL 在 MySQL 上永远可执行。
    source = Column(String(300), nullable=False, default="", server_default="")

    # 可空：与既有 models/interview.py 的 created_at 写法保持一致
    # （由 ORM 侧 default=datetime.utcnow 赋值，不依赖数据库默认值）
    created_at = Column(DateTime, default=datetime.utcnow)


class KnowledgeChunk(Base):
    """知识切片从表：检索的最小单位，``document_id`` 指回所属文档。

    ``metadata`` 的 Python 属性名是 ``chunk_metadata``
    ------------------------------------------------
    ``metadata`` 是 **SQLAlchemy Declarative 的保留属性名**
    （``Base.metadata`` 是全局 ``MetaData`` 对象），直接写
    ``metadata = Column(...)`` 会抛 ``InvalidRequestError``。
    因此用 ``Column("metadata", ...)`` 显式指定**数据库列名仍为 ``metadata``**，
    Python 侧属性改名 ``chunk_metadata``——库表结构与需求一致，只是属性名避让。

    ``embedding`` / ``embedding_model`` / ``embedding_dim``（预留的向量存储）
    ------------------------------------------------------------------------
    三列都在本阶段**只建结构、不写入**（写向量是 Embedding 服务的下一步）。

    - ``embedding``：向量本体，存 ``List[float]``。用 **JSON** 而不是 pgvector 之类
      专用类型，因为运行库是 **MySQL 8**——JSON 在 MySQL / SQLite 上都能用且零扩展。
    - ``embedding_model``：**哪次编码产生的**。换模型后旧向量与新向量**不可比**
      （维度可能不同、语义空间也不同），必须能按模型筛出待重算的行。
      非空 + ``server_default=""``，于是「老库补列」在**有数据的表上**也能执行。
    - ``embedding_dim``：向量维度，冗余但有用——MySQL 里查 JSON 数组长度很别扭，
      单列才能直接用 SQL 找出「维度不对/模型已换」的切片。

    三者口径：**可空表示「尚未向量化」**，非空表示「已用 ``embedding_model`` 编码过」。
    因此 ``embedding IS NULL`` 就是「待向量化」的筛选条件。
    """

    __tablename__ = "knowledge_chunk"

    id = Column(Integer, primary_key=True, autoincrement=True)

    document_id = Column(
        Integer,
        ForeignKey("knowledge_document.id"),
        nullable=False,
        index=True,
    )

    content = Column(Text, nullable=False)

    # 数据库列名保持 metadata（与需求一致）；Python 属性必须改名，见类文档
    chunk_metadata = Column("metadata", JSON, nullable=False, default=dict)

    # ---- 向量存储（预留：本阶段只建结构，由 EmbeddingService 负责写入）----
    # 向量本体：可空 = 尚未向量化（"embedding IS NULL" 即「待处理」）
    embedding = Column(JSON, nullable=True)
    # 编码该向量的模型标识（换模型后据此找旧向量重算）；与 interview_mode 同款
    # default + server_default 双写，保证「新建表」与「老库补列」落出同样的默认值
    embedding_model = Column(
        String(100), nullable=False, default="", server_default=""
    )
    # 向量维度（便于用 SQL 直接筛出维度不符的切片）
    embedding_dim = Column(Integer, nullable=True)


__all__ = [
    "KNOWLEDGE_CATEGORIES",
    "KNOWLEDGE_CATEGORY_COMPANY",
    "KNOWLEDGE_CATEGORY_JOB",
    "KNOWLEDGE_CATEGORY_LABELS",
    "KNOWLEDGE_CATEGORY_PROJECT",
    "KNOWLEDGE_CATEGORY_TECHNICAL",
    "KnowledgeChunk",
    "KnowledgeDocument",
]
