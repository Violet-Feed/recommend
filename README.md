### 提供接口

1. 上报热点：sentinel热点检测，写入热点redis
2. 上报搜索词：调用大模型获取主题，embedding，从milvus召回相似，热点检测
3. 上报embedding：embedding，写入milvus
4. 上报用户行为：写入redis？
5. 推荐系统：基于用户行为从milvus召回，消重