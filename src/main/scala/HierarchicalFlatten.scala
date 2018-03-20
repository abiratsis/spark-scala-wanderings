object HierarchicalFlatten extends App {
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession

  val sparkConf = new SparkConf().setAppName("pathtest").setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  var dfA = spark.createDataset(Seq(
    (55, "Canada", -1, "", 0),
    (77, "Ontario", 55, "/55", 1),
    (100, "Toronto", 77, "/55/77", 2),
    (104, "Brampton", 100, "/55/77/100", 3))
  ).toDF("Id", "name", "parentId", "path", "depth")

  def getArray = udf((path: String) => {
    if (path.contains("/"))
      path.split("/")
    else
      Array[String](null)
  })

  val dfB = dfA
    .withColumn("path", getArray(col("path")))
    .withColumn("path", explode(col("path")))
    .toDF()

  //    dfB.show()
  dfB.as("B").join(dfA.as("A"), $"B.parentId" === $"A.Id", "left")
    .select($"B.Id".as("Id"), $"B.name".as("name"), $"A.name".as("parent"), $"B.depth".as("depth"))
    .show()
}
