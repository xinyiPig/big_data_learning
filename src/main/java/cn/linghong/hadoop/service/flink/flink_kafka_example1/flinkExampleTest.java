package cn.linghong.hadoop.service.flink.flink_kafka_example1;

import cn.linghong.hadoop.config.KafkaConfig;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Slf4j
@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)

public class flinkExampleTest {


    /**
    * @Description: 读取kafka,一条条存到mysql
    * @Param:
    * @return:
    * @Author: zzx
    * @Date: 2020-08-18
    **/
    @Test
    public   void test() throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaConfig.getKafkaSourceProperty();
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                props.getProperty("topic"),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(Integer.valueOf(props.getProperty("parallelism")))
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象
        SingleOutputStreamOperator<Student> transitStream ;
        // 数据转换 例子 start

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
        // map end

        // filter 函数根据条件判断出结果
//         transitStream = student.filter(new FilterFunction<Student>() {
//             @Override
//             public boolean filter(Student student) throws Exception {
//                 if(student.id>96){
//                     return true;
//                 }else{
//                     return  false;
//                 }
//             }
//         });
        // filter end

        // flatMap 作用类似 map和filter的特殊情况,返回0个，1个或多个记录，但map和filter语义更明确
//         transitStream = student.flatMap(new FlatMapFunction<Student, Student>() {
//             @Override
//             public void flatMap(Student student, Collector<Student> out) throws Exception {
//                 if(student.id%2 ==0){
//                     out.collect(student);
//                 }
//             }
//         });
        // flatMap end

//        keyBy 返回keyedStream 用student 的age作为分区的key 参考：https://www.jianshu.com/p/0cdf1112d995

        KeyedStream<Student,Integer> keyedStream = student.keyBy(new KeySelector<Student,Integer>(){
            @Override
            public Integer getKey(Student student) throws Exception{
                return student.age;
            };
        });
//        keyedStream.print();

        // keyby类似于sql中的group by，将数据进行了分组。后面基于keyedSteam的操作，如map,filter 都是组内操作
//        keyBy end

        // reduce 返回单个的结果值，并且reduce 操作每处理一个元素总是创建一个新值，例如 average,sum,min,max,count 都可以用reduce实现
        // keydStream才能进行reduce操作

//        transitStream = student.keyBy(new KeySelector<Student,Integer>(){
//            @Override
//            public Integer getKey(Student student) throws Exception{
//                return student.age;
//            };
//        }).reduce(new ReduceFunction<Student>() {
//            @Override
//            public Student reduce(Student t1, Student t2) throws Exception {
//                Student t3 = new Student();
//                t3.age=(t1.age+t2.age)/2;
//                return  t3;
//            }
//        });

        // Aggregations start
//        keyedStream.sum(0);
//        keyedStream.sum("key");
//        keyedStream.min(0);
//        keyedStream.min("key");
//        keyedStream.minBy(0);
//        keyedStream.minBy("key");

        // min与minBy的区别在于，minBy同时保留其他字段的数值，min算子对该字段求最小值，并将结果保存在该字段上。对于其他字段，该操作并不能保证其数值
        // Aggregations end

        // union 函数将两个或者多个数据流结合起来，union连接的流的类型必须一致
        // inputStream.union(inputStream s1,inputSteam s2)

        // connect之后生成ConnectedStreams，会对两个流的数据应用不同的处理方法，并且双流 之间可以共享状态(比如计数)。这在第一个流的输入会影响第二个流时, 会非常有用
        // connect只能连接两个流,connect连接的两个流类型可以不一致

        // join  通过一些key 将同一个window 的两个数据流join起来
        // 在5秒的窗口中连接两个流，其中第一个流的第一个属性的连接条件等于另一个流的第二个属性
        // inputStream.join(inputSteam1).where(0).equalTo(1).windowTime(Time.seconds(5)).apply(new JoinFunction(){...});



        // 输出transitStream
        transitStream.print();

        transitStream.addSink(new SinkToMySql()); //数据 sink 到 mysql

        env.execute("Flink add sink");

    }


    /**
    * @Description: 读取kafka 数据，批量存到mysql
    * @Param:
    * @return:
    * @Author: zzx
    * @Date: 2020-08-18
    **/
    @Test
    public  void testBatch() throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaConfig.getKafkaSourceProperty();
        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象); //
        student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    System.out.println("1 分钟内收集到 student 的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        });
    }

    @Test
    public  void createStream() throws Exception{
        KafkaUtil.writeToKafka();
    }



}