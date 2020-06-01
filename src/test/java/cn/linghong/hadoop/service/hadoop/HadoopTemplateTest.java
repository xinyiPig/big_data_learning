package cn.linghong.hadoop.service.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)

public class HadoopTemplateTest {

    @Resource
    private  HadoopTemplate hadoopTemplate;

    @Test
    public void copyFileToHDFS(){
        String hdfsPath="hdfs://localhost:9000/user/hadoop/";
        String srcFile="/Users/zzx/Desktop/激活码.txt";
        try {
            if (hadoopTemplate.existDir(hdfsPath, true)) {
                hadoopTemplate.copyFileToHDFS(false, true, srcFile, hdfsPath);
            } else {
                log.error("==============dir create fail.==============");
            }
        }catch (IOException e){
            log.info(e.getMessage());
        }
    }
}