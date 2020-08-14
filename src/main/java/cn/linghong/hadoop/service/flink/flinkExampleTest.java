package cn.linghong.hadoop.service.flink;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


@Slf4j
@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)

public class flinkExampleTest {

    /**
    * @Description: 此例子基于flink 单机模式 macOs 先在命令行执行 nc -l 9000
    * @Param:
    * @return:
    * @Author: zzx
    * @Date: 2020-08-03
    **/
    @Test
    public   void test1(){
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         *  env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         *  .filter()
         *  .flatMap()
         *  .join()
         *  .coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

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