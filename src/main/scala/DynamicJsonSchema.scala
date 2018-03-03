import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.native.JsonMethods

import scala.collection.mutable.ListBuffer

object DynamicJsonSchema extends App {

  val sparkConf = new SparkConf().setAppName("dynamic-json-schema").setMaster("local")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  case class JColumn(trim: Boolean, name: String, nullable: Boolean, id: Option[String], position: BigInt, table: String, _type: String, primaryKey: Boolean)

  val path = """C:\Users\abiratsis.OLBICO\Desktop\schema.json"""
  val input = scala.io.Source.fromFile(path)
  val json = JsonMethods.parse(input.reader())

  val typeMapping = Map(
    "double" -> DoubleType,
    "integer" -> IntegerType,
    "string" -> StringType,
    "date" -> DateType,
    "bool" -> BooleanType)

  var rddSchema = ListBuffer[StructField]()
  implicit val formats = DefaultFormats
  val schema = json.extract[Array[JColumn]]

  //schema.foreach(c => println(s"name:${c.name} type:${c._type} isnullable:${c.nullable}"))

  schema.foreach { c =>
    rddSchema += StructField(c.name, typeMapping(c._type), c.nullable, Metadata.empty)
  }

  val in_emp = spark.read
    .format("com.databricks.spark.csv")
    .schema(StructType(rddSchema.toList))
    .option("inferSchema", "false")
    .option("dateFormat", "yyyy.MM.dd")
    .option("header", "false")
    .option("delimiter", ",")
    .option("nullValue", "null")
    .option("treatEmptyValuesAsNulls", "true")
    .csv("""C:\Users\abiratsis.OLBICO\Desktop\employee.csv""")

  in_emp.printSchema()
  in_emp.collect()
  in_emp.show()

}

