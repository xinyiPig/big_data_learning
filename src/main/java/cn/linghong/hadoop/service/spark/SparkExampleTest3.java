package cn.linghong.hadoop.service.spark;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.HashMap;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
/**
 * @Description: 广工打卡数据的Spark数据处理与分析
 * @Param:
 * @return:
 * @Author: zzx
 * @Date: 2020-07-14
 **/
public class SparkExampleTest3 implements Serializable {

    @Test
    public void main() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .appName("sparkTemplate")
                .getOrCreate();
//        连接数据库
        Dataset jdbcDF = connectToMysql(spark);
//        把数据转成临时表，并分析
        studentStatistics(spark, jdbcDF);
    }

    // 数据库连接
    public Dataset connectToMysql(SparkSession spark) {
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/big_data?useUnicode=true&characterEncoding=utf-8&useSSL=false")
                .option("dbtable", "t_record_per_day")
                .option("user", "root")
                .option("password", "rootroot")
                .load();
        return jdbcDF;
    }

    // 把文件转成临时视图,统计老师的相关信息
    public void teacherStatistics(SparkSession spark, Dataset df) {
        df.createOrReplaceTempView("data");
//        Dataset resultDF = spark.sql("SELECT\n" +
//                "\tfrom_unixtime(submit_date/1000, 'HH') as HOUR," +
//                "count(id) COUNT " +
//                "FROM\n" +
//                "\tdata\n" +
//                " GROUP BY HOUR ORDER BY HOUR");
////        showDataFrame(resultDF);
//        resultDF.repartition(1).write().json("file:///Users/zzx/Downloads/big_data/teacher_record_per_day/1.json");

        Dataset resultDF1 = spark.sql("SELECT\n" +
                "\tcensus_province as province," +
                "count(id) COUNT " +
                "FROM\n" +
                "\tdata\n" +
                " GROUP BY province ORDER BY province");
//        showDataFrame(resultDF);
        resultDF1.repartition(1).write().json("file:///Users/zzx/Downloads/big_data/teacher_record_per_day/2.json");

    }
    // 把文件转成临时视图,统计老师的相关信息
    public void studentStatistics(SparkSession spark, Dataset df) {
        df.createOrReplaceTempView("data");
        Dataset resultDF = spark.sql("SELECT\n" +
                "\tfrom_unixtime(submit_date/1000, 'HH') as HOUR," +
                "count(id) COUNT " +
                "FROM\n" +
                "\tdata\n" +
                " GROUP BY HOUR ORDER BY HOUR");
//        showDataFrame(resultDF);
        resultDF.repartition(1).write().json("file:///Users/zzx/Downloads/big_data/student_record_per_day/1.json");

//        Dataset resultDF1 = spark.sql("SELECT\n" +
//                "\tcensus_province as province," +
//                "count(id) COUNT " +
//                "FROM\n" +
//                "\tdata\n" +
//                " GROUP BY province ORDER BY province");
////        showDataFrame(resultDF);
//        resultDF1.repartition(1).write().json("file:///Users/zzx/Downloads/big_data/teacher_record_per_day/2.json");

    }

    //    展示df
    public void showDataFrame(Dataset<Row> df) {
        Dataset<String> namesDS = df.map(
                (MapFunction<Row, String>) row -> "Name: " + row, Encoders.STRING());
        namesDS.show();
    }

    public Date stringToDate(String str) {
        Date value = null;
        try {
            SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy/MM/dd");
            Long timeStamp = formatter1.parse(str).getTime();
            value = new Date(timeStamp);

        } catch (Exception e) {

        }
        return value;
    }

}
