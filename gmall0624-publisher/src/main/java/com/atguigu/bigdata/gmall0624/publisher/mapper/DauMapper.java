package com.atguigu.bigdata.gmall0624.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
        //求日活总数
        public Long getDauTotal(String data);
        //求分时活跃数
        public List<Map> getDauTotalHourMap(String data);

}
