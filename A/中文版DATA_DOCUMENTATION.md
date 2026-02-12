一、数据总体说明
本项目使用两个独立来源的 Reddit 数据集：
SocialGrep Reddit Dataset
Pushshift Reddit Dataset
两个数据集来源不同、规模不同、结构不同、score 计算方式可能不同，因此不可直接混合分析。
二、SocialGrep Reddit Dataset
数据来源：
https://huggingface.co/datasets/SocialGrep/the-reddit-dataset-dataset
原始规模：
约 75.1M rows（posts + comments）
处理方式：
全量 streaming 遍历
应用清洗规则
最终保留 63,079 rows
2.1 输出文件
data/processed/base_comments.parquet
data/processed/base_post_internal.parquet
data/processed/base_post_external.parquet
data/processed/base_all.parquet
2.2 清洗规则
对 SocialGrep 数据进行了以下清洗：
删除空文本字段
删除被删除内容：
“[deleted]”
“[removed]”
“removed by reddit”
删除文本长度 < 20 字符的数据
去重 (type, id)
标准化字符串
可选 UTC 时间转换
2.3 kind 与 type 说明
所有文件均包含：
字段	类型	说明
kind	string	结构类别
type	string	逻辑类别
type 取值：
"post"
"comment"
kind 取值：
"comment"
"post_internal"
"post_external"
解释：
post_internal：Reddit 站内原创内容，有 selftext
post_external：外链内容，无 selftext，有 url + domain
comment：评论
2.4 各文件字段结构（含数据类型）
（1）base_comments.parquet
属于：comment
字段	数据类型	说明
kind	string	固定为 "comment"
type	string	固定为 "comment"
id	string	Reddit base-36 ID
created_utc	int64 或 datetime	发布时间
score	int64	评论分数
permalink	string	Reddit 永久链接
body	string	评论文本
sentiment	float 或 int 或 null	情感标签
（2）base_post_internal.parquet
属于：post
类型：internal（Reddit 站内原创内容，有 selftext）
字段	数据类型	说明
kind	string	"post_internal"
type	string	"post"
id	string	Post ID
created_utc	int64 或 datetime	发布时间
score	int64	帖子分数
permalink	string	链接
selftext	string	正文
title	string	标题
行数：10,928
（3）base_post_external.parquet
属于：post
类型：external（外链内容，无 selftext，有 url + domain）
字段	数据类型	说明
kind	string	"post_external"
type	string	"post"
id	string	Post ID
created_utc	int64 或 datetime	发布时间
score	int64	帖子分数
permalink	string	链接
title	string	标题
url	string	外部链接
domain	string	外部网站域名
行数：4,600
（4）base_all.parquet
行数：63,079
磁盘大小：12.8 MB
该文件为合并后的总表。
合并来源：
base_post_internal.parquet
base_post_external.parquet
base_comments.parquet
即合并了：
post_internal + post_external + comment
字段	数据类型	说明
kind	string	内容结构类别
type	string	post/comment
id	string	唯一 ID
subreddit_id	string	子版块 ID
subreddit_name	string	子版块名称
created_utc	int64 或 datetime	时间
score	int64	分数
permalink	string	链接
body	string 或 null	评论文本
sentiment	float/int/null	情感
selftext	string 或 null	正文
title	string 或 null	标题
url	string 或 null	外部链接
domain	string 或 null	域名
缺失字段统一填充为 NaN。比如post没有body，标为 NaN。
2.5 子版块说明
什么是子版块（Subreddit）？
Reddit 是一个由多个主题社区组成的平台，每个社区称为一个“子版块”（subreddit），通常以：
r/名称
表示。
例如：
r/datasets
就是一个专门讨论数据集的子版块。
每条帖子或评论都属于某一个子版块。
SocialGrep 子版块特性
所有数据均来自：
subreddit_id = 2r97t
subreddit_name = datasets
即全部来自：
r/datasets
因此：
无跨 subreddit 数据
不适合做社区间对比分析
三、Pushshift Reddit Dataset
数据来源：
https://huggingface.co/datasets/fddemarco/pushshift-reddit
原始规模：
约 550M rows
该数据集本身只包含 post（不包含 comment）。
处理方式：
Streaming 采样
抽样保留 200,000 rows，如需更多数据可联系我。
selftext ≥ 20 字符
输出文件：
data/processed/pushshift-reddit_post.parquet
3.1 文件结构
行数：200,000
磁盘大小：75.35 MB
内存占用：137.95 MB
字段	数据类型	说明
kind	string	固定为 "post_internal"
type	string	固定为 "post"
id	string	Reddit ID
subreddit_id	string	子版块 ID
subreddit_name	string	子版块名称
created_utc	int64 或 datetime	时间
score	int64	帖子分数
selftext	string	正文
title	string	标题
num_comments	int64	评论数量
3.2 子版块特性
Pushshift 数据：
包含多个 subreddit
subreddit_id 与 subreddit_name 在不同 row 中不同
可用于跨社区分析
四、score 字段差异说明（重要）
SocialGrep 与 Pushshift 的 score 字段：
来源不同
抓取时间不同
计算逻辑可能不同
快照时间点可能不同
因此：
两个数据集的 score 不可直接比较或合并。
推荐做法：
分开建模分析
或在各自数据集内部做标准化（如 z-score）
五、项目目录说明
Research-about-AI-vs-Human-engagement/
│
├── data/
│   └── processed/
│       ├── pushshift-reddit_post.parquet
│       ├── base_comments.parquet
│       ├── base_post_internal.parquet
│       ├── base_post_external.parquet
│       └── base_all.parquet
│
├── src/
│   ├── pushshift-reddit_fetch_split.py
│   └── the-reddit-dataset-dataset_fetch_split.py
│
└── 中文版DATA_DOCUMENTATION.md

脚本说明
pushshift-reddit_fetch_split.py
用于处理：
Pushshift Reddit Dataset
（fddemarco/pushshift-reddit）
功能：
Streaming 抽样
清洗 selftext
输出 pushshift-reddit_post.parquet
the-reddit-dataset-dataset_fetch_split.py
用于处理：
SocialGrep Reddit Dataset
（SocialGrep/the-reddit-dataset-dataset）
功能：
全量 streaming
分类 post_internal / post_external
清洗 comment
输出 4 个 parquet 文件
