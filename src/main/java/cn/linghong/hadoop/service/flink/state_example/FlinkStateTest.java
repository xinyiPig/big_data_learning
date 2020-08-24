package cn.linghong.hadoop.service.flink.state_example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@Slf4j
@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)

public class FlinkStateTest {


    /**
     * @Description: 我们把元组的第一个元素当作 key（在示例中都 key 都是 “1”）。 该函数将出现的次数以及总和存储在
     * “ValueState” 中。 一旦出现次数达到 2，则将平均值发送到下游，并清除状态重新开始，每个不同的 key（元组中第一个元素）保存一个单独的值。
     * @Param:
     * @return:
     * @Author: zzx
     * @Date: 2020-08-18
     **/
    @Test
    public   void testKeyState() throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .flatMap(new CountWindowAverage()).print();

        env.execute("testKeyState");
    }





}