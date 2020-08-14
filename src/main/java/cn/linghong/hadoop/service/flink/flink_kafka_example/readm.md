这是从kafka去获取数据的例子
* 步骤一：kafka下 启动zookeeper kafka。kafka需要与scala捆绑的版本
 * bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
 * bin/kafka-server-start.sh -daemon config/server.properties
 * 步骤二：在 Run/ Edit Configurations 中勾选 Include dependencies with “Provided” scope。
 * 步骤三：分别启动KafkaUtils FlinkKafkaTest