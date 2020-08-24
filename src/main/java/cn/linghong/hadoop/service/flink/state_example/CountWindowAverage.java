package cn.linghong.hadoop.service.flink.state_example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {


    // 声明state
    private transient ValueState<Tuple2<Long,Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 描述state
        ValueStateDescriptor<Tuple2<Long,Long>> descriptor = new ValueStateDescriptor<>(
                "average",// the state name
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                Tuple2.of(0L,0L)); // 默认值
        // 赋值
        sum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        // 使用
        Tuple2<Long,Long> currentSum = sum.value();

        // 更新次数
        currentSum.f0+=1;

        currentSum.f1+=input.f1;

        if(currentSum.f0>=2){
            out.collect(new Tuple2<>(input.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }

    }
}