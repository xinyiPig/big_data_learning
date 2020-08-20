package cn.linghong.hadoop.service.flink.Demo;


import cn.linghong.hadoop.config.KafkaConfig;
import cn.linghong.hadoop.service.flink.flink_kafka_example1.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.util.Properties;

public class CheckPointState_Demo {
    private  static String hdfsPath = "hdfs://localhost:9000/flink/checkpoint/";

    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置checkPoint 1分钟
        env.enableCheckpointing(1000);
        // 设置模式为 exactly-once(默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 确保检查点之间至少有500ms的间隔【checkpoint 最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在10秒内完成，过时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 设置state backend
        try {
            env.setStateBackend(new RocksDBStateBackend(hdfsPath,true));
        }catch (IOException e){
            e.printStackTrace();
        }
        Properties props = KafkaConfig.getKafkaSourceProperty();
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                props.getProperty("topic"),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(Integer.valueOf(props.getProperty("parallelism")))
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象
        SingleOutputStreamOperator<Student> transitStream ;
        // 数据转换
        // Map 每个人的年龄+5 对所有数据处理
        transitStream= student.map(new MapFunction<Student, Student>() {
            @Override
            public Student map(Student student) throws Exception {
                Student s1 = new Student();
                BeanUtils.copyProperties(s1,student);
                s1.age = s1.age+5;
                return s1;
            }
        });
        transitStream.print();
        try{
            env.execute("stateExecute");
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
