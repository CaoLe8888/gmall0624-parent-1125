package com.atguigu.bigdata.gmall0624.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.bigdata.gmall0624.publisher.service.PublisherService;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date) {

        List<Map> list = new ArrayList<Map>();
        // 日活总数
        Long dauTotal = publisherService.getDauTotal(date);
        Map dauMap=new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        list.add(dauMap);

        // 新增用户
        Map newMidMap=new HashMap<String,Object>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",233L);
        list.add(newMidMap);

        return JSON.toJSONString(list);
    }
    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String td){
       if("dau".equals(id)){
           String yd = getYd(td);
           Map dauTotalHourMapTD = publisherService.getDauTotalHourMap(td);
           Map dauTotalHourMapYD = publisherService.getDauTotalHourMap(yd);

           Map hourMap =new HashMap();
           hourMap.put("td",dauTotalHourMapTD);
           hourMap.put("yd",dauTotalHourMapYD);

           return JSON.toJSONString(hourMap);
       }else{
                return  null;
       }
    }
    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date tody = dateFormat.parse(td);
            Date ystd = DateUtils.addDays(tody, -1);
            return  ystd.toString();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
