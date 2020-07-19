package cn.linghong.hadoop.service.spark;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
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
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.asc;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
/**
 * @Description: 基于零售交易数据的Spark数据处理与分析
 * @Param:
 * @return:
 * @Author: zzx
 * @Date: 2020-07-14
 **/
public class SparkExampleTest2 implements Serializable {

    @Test
    public void main() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .appName("sparkTemplate")
                .getOrCreate();
        // 数据清洗完以后就可以不再调用了，不然会报文件已存在的异常
        // this.dataCleaning(spark);
        transFileToTempView(spark);
    }

    // 数据清洗
    public void dataCleaning(SparkSession spark){

        HashMap<String,String> dataFrameOptions=new HashMap<>();
        dataFrameOptions.put("header","true");
        dataFrameOptions.put("inferschema","true");
        //（1）读取在磁盘上的文件，以csv的格式读取，得到DataFrame对象。
        Dataset df = spark.read().format("com.databricks.spark.csv").options(dataFrameOptions).load("file:///Users/zzx/Downloads/big_data/E_Commerce_Data.csv");
        // (2)查看行数,查看数据集的大小，输出541909，不包含标题行
        System.out.println(df.count());
        // （3）打印数据集的schema，查看字段及其类型信息
        System.out.println(df.schema());
        // （4）创建临时视图data
        df.createOrReplaceTempView("data");
        //（5）由于顾客编号CustomID和商品描述Description均存在部分缺失，所以进行数据清洗，过滤掉有缺失值的记录。
        // 特别地，由于CustomID为integer类型，所以该字段若为空，则在读取时被解析为0，故用[“CustomerID”]!=0 条件过滤。
        Dataset cleanDf = df.filter("CustomerID!=0").filter("Description!=''");
        // (6)查看清洗后的数据
        System.out.println(cleanDf.count());
        // 数据清洗结束。根据作业要求，预处理后需要将数据写入磁盘。将清洗后的文件以csv的格式，写入E_Commerce_Data_Clean.csv
        cleanDf.write().format("com.databricks.spark.csv").options(dataFrameOptions).save("file:///Users/zzx/Downloads/big_data/E_Commerce_Data_clean.csv");
    }

    // 把文件转成临时视图,动态创建Schema将非json格式的RDD转换成DataFrame
    public void transFileToTempView(SparkSession spark) {
        HashMap<String,String> dataFrameOptions=new HashMap<>();
        dataFrameOptions.put("header","true");
        dataFrameOptions.put("inferschema","true");
        Dataset df = spark.read().format("com.databricks.spark.csv").options(dataFrameOptions).load("file:///Users/zzx/Downloads/big_data/E_Commerce_Data_clean.csv");
        df.createOrReplaceTempView("data");
        Dataset countryCustomerDF = spark.sql("SELECT Country,COUNT(DISTINCT CustomerID) AS countOfCustomer FROM data GROUP BY Country ORDER BY countOfCustomer DESC LIMIT 10");
        showDataFrame(countryCustomerDF);
    }

//    展示df
    public void showDataFrame(Dataset<Row> df) {
        Dataset<String> namesDS = df.map(
                (MapFunction<Row, String>) row -> "Name: " + row, Encoders.STRING());
        namesDS.show();
    }

    public Date stringToDate(String str){
        Date value=null;
        try{
            SimpleDateFormat formatter1=new SimpleDateFormat("yyyy/MM/dd");
            Long timeStamp = formatter1.parse(str).getTime();
            value=new Date(timeStamp);

        }catch (Exception e){

        }
       return value;
    }

}
