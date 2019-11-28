package com.atguigu.bigdata.gmall0624.publisher.service.impl;

import com.atguigu.bigdata.gmall0624.publisher.mapper.DauMapper;
import com.atguigu.bigdata.gmall0624.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String data) {
        Long dauTotal = dauMapper.getDauTotal(data);
        return dauTotal;
    }

    @Override
    public Map getDauTotalHourMap(String data) {
        List<Map> hourMap = dauMapper.getDauTotalHourMap(data);
        Map dauhourMap = new HashMap();
        for (Map map : hourMap) {
            dauhourMap.put(map.get("loghour"), map.get("ct"));
        }
        return dauhourMap;
    }
}
