package com.atguigu.bigdata.gmall0624.realtime.ap
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.gmall0624.realtime.bean.StartUpLog
import com.atguigu.bigdata.gmall0624.realtime.util.MyKafkaUtil
import common.GmallConstants
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val startupStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    //           startupStream.map(_.value()).foreachRDD{ rdd=>
    //             println(rdd.collect().mkString("\n"))
    //           }
    val startLogDS: DStream[StartUpLog] = startupStream.map(record => {
      //取出流中的值
      val jsonString: String = record.value()
      //根据样例类  反射转化为对象
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      //补时间戳
      val date: Date = new Date(startUpLog.ts)
      //时间格式工具
      val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      //时间格式化
      val dataString: String = simpleDateFormat.format(date)
      //切开为日期和天数两部分
      val dataArr: Array[String] = dataString.split(" ")
      //赋值封装
      startUpLog.logDate = dataArr(0)
      startUpLog.logHour = dataArr(1)
      //返回
      startUpLog
    })

    //todo  过滤去重  链接不要写在算子内 每条数据都会执行一次
    //transform 写的可以在diver端周期的执行
    val filterDS: DStream[StartUpLog] = startLogDS.transform(rdd => {
      println("过滤前:"+rdd.count())
      //diver
      val jedis: Jedis = new Jedis("hadoop103", 6379)
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date: String = dateFormat.format(new Date())
      val dataKey: String = "dau:" + date
      //print(dataKey)
      //查出已经存在的value
      val midSet: util.Set[String] = jedis.smembers(dataKey)
      jedis.close()
      //通过广播变量发送到executor端
      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      //过滤操作
      val filterRDD: RDD[StartUpLog] = rdd.filter(startLog => {
        val midSet: util.Set[String] = dauMidBC.value
        //executor端进行比对
        !midSet.contains(startLog.mid)
      })
      println("过滤后:"+filterRDD.count())
      filterRDD
    })
    //todo    批次内去重
    //根据mid分组  取小组第一个
    val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] =
                              filterDS.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val distictDS: DStream[StartUpLog] = groupbyMidDstream.flatMap {
      case (mid, startupLogItr) =>
        startupLogItr.toList.take(1)
    }
    //进行缓存 提高性能
    //    distictDS.cache()

    //todo 保存  redis
    distictDS.foreachRDD(rdd=>{
      //使用foreachPartition 批处理  减少连接次数
      rdd.foreachPartition(startlog=>{
        val jedis: Jedis = new Jedis("hadoop103",6379)
        //遍历startlog 对每一条数据进行操作
        for (elem <- startlog) {
          //set 的写入
          //拼装redis中的key
          val dataKey: String = "dau:"+elem.logDate
          //保存到redis，存入的为设备ID、
          jedis.sadd(dataKey,elem.mid)
        }
        //关闭jedis的链接
        jedis.close()
      })
    })

    //TODO  保存到hbse
    import org.apache.phoenix.spark._
    distictDS.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL0624_DAU",Seq("MID", "UID", "APPID", "AREA",
                                  "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
                                  new Configuration(),Some("hadoop103,hadoop104,hadoop105:2181"))
    })
    //启动
    ssc.start()
    //等待关闭
    ssc.awaitTermination()
  }
}
