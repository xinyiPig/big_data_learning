package cn.linghong.hadoop.service.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.io.IOException;


@Slf4j
public class CheckPointStateDemo {

    private  static String hdfsPath = "hdfs://localhost:9000/flink/checkpoint/";

    /**
    * @Description: 此例子基于flink 单机模式 macOs 先在命令行执行 nc -l 9000
    * @Param:
    * @return:
    * @Author: zzx
    * @Date: 2020-08-03
    **/
    public static void main(String[] args){
        // set up the streaming execution environment
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
        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000);
        DataStream<Tuple2<String, Integer>> dataStream = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split("\\W+");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        dataStream.print();
        // execute program
        try {
            env.execute("Java WordCount from SocketTextStream Example");
        }catch (Exception e){
            log.error(e.getMessage());
        }

    }


}