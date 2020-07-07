package cn.linghong.hadoop.service.spark;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.Serializable;
import java.sql.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class SparkExampleTest implements Serializable {



    @Test
    public void  getData() {
        String logFile = "file:///Users/zzx/Desktop/project/bigData/spark/README.md"; // Should be some file on your system
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName("SimpleApp");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile);
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();
        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    }


}
