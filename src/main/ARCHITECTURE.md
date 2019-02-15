DMP(数据管理平台)架构
--| 广告投放引擎
   --| 与广告交易平台交互
      --| 接收ADX广告位信息
      --| 将后台的竞价广告/价格反馈给ADX
      --| 接收ADX的resultMess
   --| 将交互的数据日志缓存(人物行为/内容,广告信息)
      --| 思考:缓存在哪里???--->redis(数据量大且要求快)[参考见:REDIS.md]
      --| javaPoject把redis_to_disk保证redis正常运行
--| 数据存储(用于离线计算)
    --| flume把 disk_to_hdfs [参考见:FLUME.md,HDFS.md]
--| 离线计算(spark)    
    --| 离线报表
        --| 落地到MySql
    --| 用户画像
        --| habse+ec/redis
        
        
        
DMP(数据管理平台)开发要点
--| 日志转化   
    --| 要求一: 原始日志文件bzip格式转化为parquet    
    --| 要求二: 序列化方式才有kyroSerializer  
    --| 要求三: parquet文件采用Snappy压缩(默认gzip) 
--| 各省市数据分布情况    