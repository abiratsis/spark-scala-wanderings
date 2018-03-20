
object RegExMatch {
  import org.apache.spark.sql.functions._
  import java.util.regex.Pattern

  final val usdPattern = Pattern.compile("USD (\\d+),?(\\d+).\\d+")
  def getMatches = udf((text: String, indx :Int) => {
    val matcher = usdPattern.matcher(text)
    var result:String = ""

    var i = 0
    while (matcher.find()){
      if(indx == i)
        result = matcher.group(0)
      i += 1
    }
    result
  })

  case class Person(name: String,phone: String,address :Address)
  case class Address(street: String,number: String,postcode :String)
  def main( args:Array[String] ):Unit = {
    import org.apache.spark.{SparkConf}
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("regex-match").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val jsonDF = spark.read
      .json("""C:\Users\abiratsis\Desktop\regex-match.json""")
      .as[Person]
      .toDF()

    jsonDF
      .withColumn("priceInDollars", getMatches(col("price"), lit(2)))
      .select("*")
      .show(5)

  }

}
