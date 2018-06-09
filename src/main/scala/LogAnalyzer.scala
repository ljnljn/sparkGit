import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Apache日志分析
  * Created by ibf on 01/15.
  */
object LogAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Analyse Log").setMaster("local")
    val sc = new SparkContext(conf)
    // hello world
    // ================日志分析具体代码==================
    // HDFS上日志存储路径
    val path = "D://sparkLearnData/access.log"

    // 创建rdd
    val rdd = sc.textFile(path)
    //val firstData = rdd.take(0).toString
    //println(ApacheAccessLog.isValidateLogLine(firstData))
    // rdd转换，返回进行后续操作
    val apacheAccessLog: RDD[ApacheAccessLog] = rdd
      // 过滤数据
      .filter(line => ApacheAccessLog.isValidateLogLine(line))
      .map(line => {
        // 对line数据进行转换操作
        ApacheAccessLog.parseLogLine(line)
      })
    // 过滤完再调用parseLogLine解析，并转化为ApacheAccessLog的对象
    //rdd.filter(line => ApacheAccessLog.isValidateLogLine(line)).collect().foreach(println)
    // 对多次时候用的rdd进行cache
    apacheAccessLog.cache()

    // 需求一：求contentsize的平均值、最小值、最大值
    /*
    * The average, min, and max content size of responses returned from the server.
    * */
    val contentSizeRDD: RDD[Long] = apacheAccessLog
      // 提取计算需要的字段数据
      .map(log => (log.contentSize))

    // 对重复使用的RDD进行cache
    contentSizeRDD.cache()

    // 开始计算平均值、最小值、最大值
    val totalContentSize = contentSizeRDD.sum()
    val totalCount = contentSizeRDD.count()
    val avgSize = 1.0 * totalContentSize / totalCount
    val minSize = contentSizeRDD.min()
    val maxSize = contentSizeRDD.max()

    // 当RDD不使用的时候，进行unpersist
    contentSizeRDD.unpersist()

    // 结果输出
    println(s"ContentSize Avg：${avgSize}, Min: ${minSize}, Max: ${maxSize}")

    // 需求二：请各个不同返回值的出现的数据 ===> wordCount程序
    /*
    * A count of response code's returned.
    * */
    val responseCodeResultRDD = apacheAccessLog
      // 提取需要的字段数据, 转换为key/value键值对，方便进行reduceByKey操作
      // 当连续出现map或者flatMap的时候，将多个map/flatMap进行合并
      .map(log => (log.responseCode, 1))
      // 使用reduceByKey函数，按照key进行分组后，计算每个key出现的次数
      .reduceByKey(_ + _)

    // 结果输出
    println(s"""ResponseCode :${responseCodeResultRDD.collect().mkString(",")}""")

    // 需求三：获取访问次数超过N次的IP地址
    // 需求三额外：对IP地址进行限制，部分黑名单IP地址不统计
    /*
    * All IPAddresses that have accessed this server more than N times.
    * 1. 计算IP地址出现的次数 ===> WordCount程序
    * 2. 数据过滤
    * */
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar", "10.0.0.153", "208-38-57-205.ip.cal.radiant.net")
    // 由于集合比较大，将集合的内容广播出去
    val broadCastIP = sc.broadcast(blackIP)
    val N = 10
    val ipAddressRDD = apacheAccessLog
      // 过滤IP地址在黑名单中的数据
      .filter(log => !broadCastIP.value.contains(log.ipAddress))
      // 获取计算需要的IP地址数据，并将返回值转换为Key/Value键值对类型
      .map(log => (log.ipAddress, 1L))
      // 使用reduceByKey函数进行聚合操作
      .reduceByKey(_ + _)
      // 过滤数据，要求IP地址必须出现N次以上
      .filter(tuple => tuple._2 > N)
    // 获取满足条件IP地址, 为了展示方便，将下面这行代码注释
    //      .map(tuple => tuple._1)

    // 结果输出
    println(s"""IP Address :${ipAddressRDD.collect().mkString(",")}""")

    // 需求四：获取访问次数最多的前K个endpoint的值 ==> TopN
    /*
    * The top endpoints requested by count.
    * 1. 先计算出每个endpoint的出现次数
    * 2. 再进行topK的一个获取操作，获取出现次数最多的前K个值
    * */
    val K = 10
    val topKValues = apacheAccessLog
      // 获取计算需要的字段信息，并返回key/value键值对
      .map(log => (log.endpoint, 1))
      // 获取每个endpoint对应的出现次数
      .reduceByKey(_ + _)
      // 获取前10个元素, 而且使用我们自定义的排序类
      .top(K)(LogSortingUtil.TupleOrdering)
    // 如果只需要endpoint的值，不需要出现的次数，那么可以通过map函数进行转换
    //      .map(_._1)

    // 结果输出
    println(s"""TopK values:${topKValues.mkString(",")}""")


    // 对不在使用的rdd，去除cache
    apacheAccessLog.unpersist()

    // ================日志分析具体代码==================

    sc.stop()
  }
}


import scala.util.matching.Regex

/**
  * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  * Created by ibf on 01/15.
  */
case class ApacheAccessLog(
                            ipAddress: String, // IP地址
                            clientId: String, // 客户端唯一标识符
                            userId: String, // 用户唯一标识符
                            serverTime: String, // 服务器时间
                            method: String, // 请求类型/方式
                            endpoint: String, // 请求的资源
                            protocol: String, // 请求的协议名称
                            responseCode: Int, // 请求返回值：比如：200、401
                            contentSize: Long // 返回的结果数据大小
                          )

/**
  * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  * Created by ibf on 01/15.
  * 提供一些操作Apache Log的工具类供SparkCore使用
  */
object ApacheAccessLog {
  // Apache日志的正则
  val PARTTERN: Regex =
    """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r
    // 此处的^代表匹配字符串开始位置，\S代表的是匹配非空白字符
    // ^(\S+) (\S+) (\S+)可以匹配上123.125.71.111 - -
    // \w匹配字母或数字或下划线或汉字 :是普通字符，就匹配:  /也是普通字符，就匹配/
    // [\w:/]+ 代表\w或者:或者/至少出现一次
  /**
    * 验证一下输入的数据是否符合给定的日志正则，如果符合返回true；否则返回false
    *
    * @param line
    * @return
    */
  def isValidateLogLine(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)

    if (options.isEmpty) {
      false
    } else {
      true
    }
  }

  /**
    * 解析输入的日志数据
    *
    * @param line
    * @return
    */
  def parseLogLine(line: String): ApacheAccessLog = {
    if (!isValidateLogLine(line)) {
      throw new IllegalArgumentException("参数格式异常")
    }

    // 从line中获取匹配的数据,PATTERN是前面定义的正则表达式，匹配上后分为9段
    val options = PARTTERN.findFirstMatchIn(line)

    // 获取matcher
    val matcher = options.get

    // 构建返回值
    ApacheAccessLog(
      matcher.group(1), // 获取匹配字符串中第一个小括号中的值
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8).toInt,
      matcher.group(9).toLong
    )
  }
}

/**
  * Created by ibf on 01/15.
  */
object LogSortingUtil {

  /**
    * 自定义的一个二元组的比较器
    */
  object TupleOrdering extends scala.math.Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      // 按照出现的次数进行比较，也就是按照二元组的第二个元素进行比较
      x._2.compare(y._2)
    }
  }

}