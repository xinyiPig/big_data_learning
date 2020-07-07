package cn.linghong.hadoop.service.hive;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import javax.sql.DataSource;
import java.sql.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class HiveExampleTest {
    @Autowired
    @Qualifier("hiveDruidDataSource")
    private DataSource druidDataSource;


    @Test
    public void  getData() {
        String sqlString="select * from course";
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        JSONObject result = null;
        try {
            conn = druidDataSource.getConnection();
            statement = conn.createStatement();
            result = new JSONObject();
            result.put("state", "0");
            JSONArray array = new JSONArray();
            rs = statement.executeQuery(sqlString.toString());
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                JSONObject jsonObj = new JSONObject();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }
                array.add(jsonObj);
            }
            result.put("analysisMACResult", array.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println( "{\"state\":\"1\"}");
        } finally {
            try {
                conn.close();
                statement.close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("{\"state\":\"1\"}");
            }
        }
        System.out.println(result.toString());

    }


}
