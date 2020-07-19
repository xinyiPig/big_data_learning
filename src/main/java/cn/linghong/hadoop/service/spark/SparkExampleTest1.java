package cn.linghong.hadoop.service.spark;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
/**
 * @Description: 新冠疫情统计例子
 * @Param:
 * @return:
 * @Author: zzx
 * @Date: 2020-07-14
 **/
public class SparkExampleTest1 implements Serializable {

    @Test
    public void main() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .appName("sparkTemplate")
                .getOrCreate();
        this.transFileToTempView(spark);
    }

    // 把文件转成临时视图,动态创建Schema将非json格式的RDD转换成DataFrame
    public void transFileToTempView(SparkSession spark) {
//        原始数据集是以.csv文件组织的，为了方便spark读取生成RDD或者DataFrame，首先将us-counties.csv转换为.txt格式文件us-counties.txt。转换操作使用python实现，代码组织在toTxt.py中，具体代码如下：
//
//import pandas as pd
//
//#.csv->.txt
//        data = pd.read_csv('/home/hadoop/us-counties.csv')
//        with open('/home/hadoop/us-counties.txt','a+',encoding='utf-8') as f:
//        for line in data.values:
//        f.write((str(line[0])+'\t'+str(line[1])+'\t'
//                +str(line[2])+'\t'+str(line[3])+'\t'+str(line[4])+'\n'))
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("file:///Users/zzx/Downloads/us-counties.txt", 1)
                .toJavaRDD();
        //        System.out.println("peopleRDD:"+peopleRDD);
        // The schema is encoded in a string
        String schemaString = "date county state cases deaths";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (
                String fieldName : schemaString.split(" ")) {
            StructField field;
            if (fieldName.equals("date")) {
                field = DataTypes.createStructField(fieldName, DataTypes.DateType, true);

            } else {
                field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            }
//            System.out.println("fieldName:"+fieldName);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        System.out.println("Convert records of the RDD (people) to Rows");

        // Convert records of the RDD (people) to Rows

        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
//            System.out.println("record:" + record);
            String[] attributes = record.split("\\t");
            Date attributes0 = stringToDate(attributes[0]);
            return RowFactory.create((attributes0), attributes[1], attributes[2], attributes[3], attributes[4]);
        });

        System.out.println("Apply the schema to the RDD");

        // Apply the schema to the RDD
        Dataset<Row> shemaUsInfo = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        shemaUsInfo.createOrReplaceTempView("usInfo");

        // SQL can be run over a temporary view created using DataFrames
//        Dataset<Row> results = spark.sql("SELECT county FROM people");

        //1.计算每日的累计确诊病例数和死亡数
        Dataset<Row> df = shemaUsInfo.groupBy("date").agg(functions.sum("cases"), functions.sum("deaths")).sort(asc("date"));

        showDataFrame(df);

        // 列重命名 因为使用agg聚合计算，原本的cases列变成 sum(cases)
        Dataset<Row> df1 = df.withColumnRenamed("sum(cases)", "cases").withColumnRenamed("sum(deaths)", "deaths");

        // 对数据进行重新分区，所以是会对数据进行打散,根据rdd的处理过程，repartition可以将分区的并行度增加，也可以将分区的并行度减少 具体参考https://www.jianshu.com/p/391d42665a30
        // 不存入hdfs，是因为json文件要喂给py去生成图片，python读取hdfs文件不太方便
        df1.repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result1.json");

        // 注册为临时表供下一步使用
        df1.createOrReplaceTempView("ustotal");

        //2.计算每日较昨日的新增确诊病例数和死亡病例数
        Dataset<Row> df2 = spark.sql("select t1.date,t1.cases-t2.cases as caseIncrease,t1.deaths-t2.deaths as deathIncrease from ustotal t1,ustotal t2 where t1.date = date_add(t2.date,1)");

        showDataFrame(df2);

        df2.sort(

                asc("date")).

                repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result2.json");

        //3.统计截止5.19日 美国各州的累计确诊人数和死亡人数
        Dataset df3 = spark.sql("select date,state,sum(cases) as totalCases,sum(deaths) as totalDeaths,round(sum(deaths)/sum(cases),4) as deathRate from usInfo  where date = to_date('2020-05-19','yyyy-MM-dd') group by date,state");

        df3.sort("totalCases").

                repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result3.json");

        df3.createOrReplaceTempView("eachStateInfo");

        //4.找出美国确诊最多的10个州
        Dataset df4 = spark.sql("select date,state,totalCases from eachStateInfo  order by totalCases desc limit 10");
        df4.repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result4.json");

        //5.找出美国死亡最多的10个州
        Dataset df5 = spark.sql("select date,state,totalDeaths from eachStateInfo  order by totalDeaths desc limit 10");
        df5.repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result5.json");

        //6.找出美国确诊最少的10个州
        Dataset df6 = spark.sql("select date,state,totalCases from eachStateInfo  order by totalCases asc limit 10");
        df6.repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result6.json");

        //7.找出美国死亡最少的10个州
        Dataset df7 = spark.sql("select date,state,totalDeaths from eachStateInfo  order by totalDeaths asc limit 10");
        df7.repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result7.json");

        // 8.统计截止5.19全美和各州的病死率
        Dataset df8 = spark.sql("select 1 as sign,date,'USA' as state,round(sum(totalDeaths)/sum(totalCases),4) as deathRate from eachStateInfo group by date union select 2 as sign,date,state,deathRate from eachStateInfo").cache();
        df8.sort("sign", "deathRate").

                repartition(1).

                write().

                json("file:///Users/zzx/Downloads/result8.json");

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
            SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
            Long timeStamp = formatter1.parse(str).getTime();
            value = new Date(timeStamp);

        } catch (Exception e) {

        }
        return value;
    }

}
