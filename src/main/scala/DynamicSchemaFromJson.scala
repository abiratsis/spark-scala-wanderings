object DynamicSchemaFromJson {

  def main(args: Array[String]): Unit = {
    import rapture.json._
    import jsonBackends.jackson._

    val jsonSchema = """{"key1":"val1","key2":"source1","key3":{"key3_k1":"key3_v1", "key3_k2":"key3_v2", "key3_k3":"key3_v3"}}"""
    val json = Json.parse(jsonSchema)

    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.types._

    val schema = ArrayBuffer[StructField]()

    schema.appendAll(List(StructField("key1", StringType), StructField("key2", StringType)))

    //    1st option: adding keys as columns
    var items = ArrayBuffer[StructField]()
    json.key3.as[Map[String, String]].foreach {
      case (k, v) => {
        items.append(StructField(k, StringType))
      }
    }
    val complexColumn = new StructType(items.toArray)
    schema.append(StructField("key3", complexColumn))

    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    val sparkConf = new SparkConf().setAppName("dynamic-json-schema").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val jsonDF = spark.read
      .schema(StructType(schema.toList))
      .json("""C:\Users\abiratsis.OLBICO\Desktop\spark-scala-wanderings\src\main\data\data.json""")
      .toDF()

    jsonDF.show()
  }


}
