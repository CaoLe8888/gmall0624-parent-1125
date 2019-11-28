package com.atguigu.gmall0624.logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String log(String logString) {
        //补时间戳  用户时间可能不准
        JSONObject json = JSON.parseObject(logString);
        json.put("ts", System.currentTimeMillis());
        //罗盘日志文件
        String jsonString = json.toJSONString();
        log.info(jsonString);
        //发送到kafka
        if(json.getString("type").equals("startup")){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }


        return "success";

    }
}
