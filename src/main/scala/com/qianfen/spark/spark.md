宽窄依赖

stage 是否发生宽依赖(groupBy) rdd.groupBy 
job 是否有action rdd.take(10)
task 分区*stage;'不能看了跑; 天
 自定义分区

自定义排序