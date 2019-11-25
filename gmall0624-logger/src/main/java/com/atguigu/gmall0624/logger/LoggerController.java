package com.atguigu.gmall0624.logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {
    @PostMapping("log")
    public String log(String logString) {

        System.out.println(logString);
        //补时间戳  用户时间可能不准
        JSONObject json = JSON.parseObject(logString);

        json.put("ts",System.currentTimeMillis());
        //罗盘日志文件
        String jsonString = json.toJSONString();
        log.info(jsonString);
        //发送到kafka
        return "success";

    }
}
