object Main{
  def main( args:Array[String] ):Unit = {
//    test()
  }

  case class dataStreamed(id: String, input: String)
  def loadAllFromDirectory() = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("TextClassification").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    // Load documents (one per line)
    val data = spark.sparkContext.wholeTextFiles("C:\\tmp\\stackoverflow\\*")

    val dataset = spark.createDataset(data)

    val tweetsDF = dataset
      .map{case (id : String, input : String) =>
        val file = id.split("@").takeRight(1)(0)
        val content = input.split(":").takeRight(2)(0)
        dataStreamed(file, content)}
      .as[dataStreamed]

    tweetsDF.printSchema()
    tweetsDF.show(10)
  }

  case class VersionInfo(version: String, count: BigInt)
  case class Results(totalUsers: Int, versionData: Array[VersionInfo])
  def jsonTest(): Unit ={
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("jsontest").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
      val versionsJSON =
        """
          |[{"version":"1.2.3.4","count":4051},
          |{"version":"1.2.3.5","count":1},
          |{"version":"1.2.4.6","count":1},
          |{"version":"2.0.0.1","count":30433},
          |{"version":"3.1.2.3","count":112195},
          |{"version":"3.1.0.4","count":11457}]
        """.stripMargin

    val versionsDS = spark.read.json(Seq(versionsJSON).toDS).as[VersionInfo]

    val userCount = 100500
    val results = spark.createDataset(List(Results(userCount, versionsDS.collect())))
    results.write.json("C:\\tmp\\test.json")
  }

  def testPivot(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("pivottest").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._
    import spark.implicits._

    var dfA = spark.createDataset(Seq(
      (1, "val1", "p1"),
      (2, "val1", "p2"),
      (3, "val2", "p3"),
      (3, "val3", "p4"))
    ).toDF("id", "name", "p_col")

        dfA.groupBy($"p_col")
          .pivot("id").agg(first("name"))
          .show()
  }

  def flattenArray(): Unit ={
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("pivottest").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    var dfB = spark.createDataset(Seq(
      (Array(992, 704, 721, 705), 0, Array(105)),
      (Array(993, 705, 722, 706), 0, Array(106))
    )).toDF("c1", "c2", "c3")

    dfB.createTempView("my_view")

    spark
      .table("my_view")
      .map { r =>
        val c1 = r.getAs[Seq[Int]]("c1")
        val c3 = r.getAs[Seq[Int]]("c3")
        (c1(0), c1(1), c1(2), c1(3), r.getInt(1), c3(0))
      }
      .repartition(1).write.mode("overwrite").option("header", "true").csv("C:\\temp\\my_output.csv")
    //    dfB
    //      .map { case (c1: Array[Int], c2: Int, c3: Array[Int]) => (c1(0), c1(1), c1(2), c1(3), c2, c3(0)) }
    //      .toDF("c1", "c2", "c3", "c4", "c5", "c6")
    //      .show()
  }

  def udfExplode(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.udf

    val sparkConf = new SparkConf().setAppName("pivottest").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    var dfB = spark.createDataset(Seq(
      ("a", 4, "1-12-2015"),
      ("b", 2, "1-12-2015")
    )).toDF("id", "credit", "date")

    val mapCredit = udf((credit: Int) => for (i <- 0 to credit) yield i)
    dfB
      .withColumn("credit", explode(mapCredit(col("credit"))))
      .show()
  }

  case class info(id: String, cost: Option[BigDecimal])
  def bigDecimalTest(): Unit = {
    import org.apache.spark.sql.types.DecimalType
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("bigdecimal").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    var dfB = spark.createDataset(Seq(
      ("a", Option(12.45)),
      ("b", Option(null.asInstanceOf[Double])),
      ("c", Option(123.33)),
      ("d", Option(1.3444))
    )).toDF("id", "cost")

    dfB
      .na.fill(0.0)
      .withColumn("cost", col("cost").cast(DecimalType(38,18)))
      .as[info]
      .show()
  }

  def hashKeyToDB(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.Row

    val sparkConf = new SparkConf().setAppName("hash-to-db").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    var cols = Seq("id", "date")
    var dfA = spark.createDataset(Seq(
      (4, "01/01/2017"),
      (2, "03/01/2017"),
      (6, "04/01/2017"),
      (1, "05/01/2017"),
      (5, "09/01/2017"),
      (3, "02/01/2017"),
      (11, "01/01/2017"),
      (12, "03/01/2017"),
      (16, "04/01/2017"),
      (21, "05/01/2017"),
      (35, "09/01/2017"),
      (13, "02/01/2017")
    )).toDF(cols: _*)

    import util.hashing.MurmurHash3._

    cols = Seq("id", "date", "db_id")
    dfA
      .withColumn("db_id", concat($"id", $"date"))
      .map { case Row(id: Int, date: String, fk: String) => (id, date, stringHash(fk)) }
      .toDF(cols: _*).show()

    dfA.foreachPartition {
      iter => iter.foreach(println) /* update db here*/
    }

  }

//  def streamingMultipleDirs(): Unit ={
//    import org.apache.spark.SparkConf
//    import org.apache.spark.sql.SparkSession
//    val conf = new SparkConf().set("spark.executor.extraClassPath", "/home/hadoop/spark/conf:/home/hadoop/conf:/home/hadoop/spark/classpath/emr/*:/home/hadoop/spark/classpath/emrfs/*:/home/hadoop/share/hadoop/common/lib/*:/home/hadoop/share/hadoop/common/lib/hadoop-lzo.jar")
//
//    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.checkpoint("/name/spark-streaming/checkpointing")
//
//    val lines = ssc.textFileStream("hdfs:///name/spark-streaming/data/")
//  }



  //12345, 2341, a465c2a, p, 2015-06-10, 2015-02-23, 2015-02-23, 2, "", 1, 98941, 1, ., 17, 21, 1, "", 67890, 4313, a465c2a, p, 2015-06-10, 2015-02-23, 2015-02-23, 2, 7391, 1, 98941, 1, ., 17, 21, 1, 01


}


class MyClass{

  def preprocess(e : Int) : Unit = {}
  def transform(): Int = 0

}

object Test{
//  import org.apache.spark.SparkConf
//  import org.apache.spark.sql.SparkSession
//  val sparkConf = new SparkConf().setAppName("dynamic-json-schema").setMaster("local")
//
//  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//  val begin = 1
//  val end = 100
//  val arr = begin.until(end).toArray
//
//  val obj_arr = for(e <- arr) yield {
//    val obj = new MyClass;
//    obj.preprocess(e);
//    obj
//  }
//  val rdd = spark.sparkContext.parallelize(obj_arr, 4)
//  rdd.map{x=>{x.transform()}}.collect()
}