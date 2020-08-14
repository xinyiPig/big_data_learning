#大数据学习（持续更新）

#### 一、学习大数据，理论基础也很重要，先从厦大数据实验室的林子雨老师的线上课程开始吧

点击 [大数据学习路线图](<http://dblab.xmu.edu.cn/post/bigdataroadmap/>)

#### 二、在基础课程中，老师会带着我们把Hadoop、HDFS、Hbase、Hive 配置安装好（必须要做好）

点击 [大数据软件安装和基础编程实践指南](<http://dblab.xmu.edu.cn/blog/2630-2/>)

#### 三、编程实践

只要步骤一、二配置好，代码就能跑起来；代码都在service 文件夹下，主要有

1、Hadoop （hadoop 配置，hdsf-site.xml,core-site.html 在resource文件下）

2、MapReduce（词频分析）

3、HBase （数据库增删查改操作，base-site.xml,hbase-env.sh在resource文件下）

4、Hive 数据仓库的配置 和数据查询

5、spark 实践例子
   - 2020年美国新冠肺炎疫情数据分析  参考 http://dblab.xmu.edu.cn/blog/2636-2/ 改成java实现，存储数据到json文件，利用python画出数据分析图
   - 基于零售交易数据的Spark数据处理与分析 参考 http://dblab.xmu.edu.cn/blog/2652-2/ 改成java 清洗数据，分析数据
   
6、增加 flink 实时数据处理 实践例子
   - test1  利用websocket模仿数据实时推流
   更新了 flink 从kafka读取数据，自定义sink,写入到mysql的实践的例子;flink_kafka_example,flink_kafka_example1