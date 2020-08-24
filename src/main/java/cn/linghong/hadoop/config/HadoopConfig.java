package cn.linghong.hadoop.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Configuration
@Slf4j
public class HadoopConfig {

    @Value("${hadoop.user}")
    private String user;

    @Value("${hadoop.hdfs.hdfs-site}")
    private String hdfsSite;

    @Value("${hadoop.hdfs.core-site}")
    private String coreSite;

    @Bean("fileSystem")
    public FileSystem createFs() throws Exception {

        System.setProperty("HADOOP_USER_NAME", user);
        //读取配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

//        log.info("===============【hadoop configuration info start.】===============");
//        log.info("【hadoop conf】: size:{}, {}", conf.size(), conf.toString());
//        log.info("【fs.defaultFS】: {}", conf.get("fs.defaultFS"));
//        log.info("【fs.hdfs.impl】: {}", conf.get("fs.hdfs.impl"));
        FileSystem fs = FileSystem.newInstance(conf);
//        log.info("【fileSystem scheme】: {}", fs.getScheme());
//        log.info("===============【hadoop configuration info end.】===============");
        return fs;
    }
}
