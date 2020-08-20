package cn.linghong.hadoop.config;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;


@Component
@Data
@Slf4j
public class KafkaConfig {

    @Value("${kafka.source:.bootstrap.servers}")
    private  String sourceBootstrapServersProp;

    @Value("${kafka.source.zookeeper.connect}")
    private  String sourceZookeeperConnectProp;

    @Value("${kafka.source.group.id}")
    private  String sourceGroupIdProp;

    @Value("${kafka.source.topic}")
    private  String sourceTopicProp;

    @Value("${kafka.source.parallelism}")
    private  String sourceParallelismProp;

    @Value("${kafka.sink.bootstrap.servers}")
    private  String sinkBootstrapServersProp;

    @Value("${kafka.sink.zookeeper.connect}")
    private  String sinkZookeeperConnectProp;

    @Value("${kafka.sink.group.id}")
    private  String sinkGroupIdProp;

    @Value("${kafka.sink.topic}")
    private  String sinkTopicProp;

    @Value("${kafka.sink.parallelism}")
    private  String sinkParallelismProp;

    @Value("${kafka.key.deserializer}")
    private  String keyDeserializerProp;

    @Value("${kafka.value.deserializer}")
    private  String valueDeserializerProp;

    @Value("${kafka.auto.offset.reset}")
    private  String autoOffsetResetProp;

    private  static String sourceBootstrapServers;

    private static  String sourceZookeeperConnect;


    private static String sourceGroupId;

    private static String sourceTopic;

    private static String sourceParallelism;

    private static String sinkBootstrapServers;

    private static String sinkZookeeperConnect;

    private static String sinkGroupId;

    private static String sinkTopic;

    private static String sinkParallelism;

    private static String keyDeserializer;

    private static String valueDeserializer;

    private static String autoOffsetReset;


    @PostConstruct
    public void init(){
        sourceBootstrapServers = this.sourceBootstrapServersProp;
        sourceGroupId = this.sourceGroupIdProp;
        sourceParallelism = this.sourceParallelismProp;
        sourceTopic = this.sourceTopicProp;
        sourceZookeeperConnect = this.sourceZookeeperConnectProp;
        sinkBootstrapServers = this.sinkBootstrapServersProp;
        sinkZookeeperConnect = this.sinkZookeeperConnectProp;
        sinkGroupId = this.sinkGroupIdProp;
        sinkTopic = this.sinkTopicProp;
        sinkParallelism = this.sinkParallelismProp;
        keyDeserializer = this.keyDeserializerProp;
        valueDeserializer = this.valueDeserializerProp;
        autoOffsetReset = this.autoOffsetResetProp;
        log.info(sourceBootstrapServers);

    }

    /**
    * @Description: 获取Kafka数据源的配置，给 flink 的source用
    * @Param:
    * @return:
    * @Author: zzx
    * @Date: 2020-08-18
    **/
    public static Properties getKafkaSourceProperty(){
        Properties props = new Properties();
        log.info(sourceBootstrapServers);
        props.put("bootstrap.servers", sourceBootstrapServers);
        props.put("zookeeper.connect", sourceZookeeperConnect);
        props.put("group.id", sourceGroupId);
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", valueDeserializer);
        props.put("auto.offset.reset", autoOffsetReset);
        props.put("topic",sourceTopic);
        props.put("parallelism",sourceParallelism);
        return  props;
    }

    /**
     * @Description: 获取Kafka接收器的配置,给 flink 的sink用
     * @Param:
     * @return:
     * @Author: zzx
     * @Date: 2020-08-18
     **/
    public static Properties getKafkaSinkProperty(){
        Properties props = new Properties();
        props.put("bootstrap.servers", sinkBootstrapServers);
        props.put("zookeeper.connect", sinkZookeeperConnect);
        props.put("group.id", sinkGroupId);
        props.put("key.deserializer", keyDeserializer);
        props.put("value.deserializer", valueDeserializer);
        props.put("auto.offset.reset", autoOffsetReset);
        props.put("topic",sinkTopic);
        props.put("parallelism",sinkParallelism);

        return  props;
    }



}
