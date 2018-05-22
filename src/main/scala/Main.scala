import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable

object Main{
  def main( args:Array[String] ):Unit = {
    Test2()
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

    val sparkConf = new SparkConf()
      .setAppName("test").setMaster("local")
      .set("spark.sql.shuffle.partitions", "1024")
      .set("spark.default.parallelism", "200")

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

    cols = Seq("id", "date", "db_id")
    dfA
      .withColumn("db_id", sha2(concat($"id", $"date"), 256))
      .toDF(cols: _*).show()

    dfA.foreachPartition {
      iter => /* update db here*/
    }

  }

  def unwrapArraysOfArrays(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    val sparkConf = new SparkConf().setAppName("dynamic-json-schema").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    import scala.collection.mutable.WrappedArray
    val df = Seq(
      (1, Seq("USD", "CAD")),
      (2, Seq("AUD", "YEN", "USD")),
      (2, Seq("GBP", "AUD", "YEN")),
      (3, Seq("BRL", "AUS", "BND", "BOB", "BWP")),
      (3, Seq("XAF", "CLP", "BRL")),
      (3, Seq("XAF", "CNY", "KMF", "CSK", "EGP")
      )
    ).toDF("ACC", "CCY")

    val castToArray = udf((ccy: WrappedArray[WrappedArray[String]]) => ccy.flatten.distinct.toArray)
    val df2 = df.groupBy($"ACC")
      .agg(collect_list($"CCY").as("CCY"))
      .withColumn("CCY", castToArray($"CCY"))
        .show(false)
  }

  def test(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession

    val sparkConf = new SparkConf().setAppName("dynamic-json-schema").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = Seq("a","b","c","d","e")
      .toDF("val1")
      .withColumn("id", monotonically_increasing_id)

    val df2 = Seq(1, 2, 3, 4, 5)
      .toDF("val2")
      .withColumn("id", monotonically_increasing_id)

    df.join(df2, "id").select($"val1", $"val2").show(false)
  }

  def Test2(): Unit ={

    var schemaInit = "id int,id2 int, kvknummer8 string, KvKnummer string, company_all string, address_only string, company_only string, duns string, Zaaknaam string, Handelsnaam string, Vennootschapsnaam string, plaatsnaam string, hierarchytree_id int, adres string, status string, concern_organisatie_type int, vsivestigings_status_indicator string, DNB_DUNS string, postcode_id int, concernlevel int, sbi_id int, awp int, activiteit_group_id1 int, activiteit_group_id2 int, activiteit_group_id3 int, activiteit_group_id4 int, activiteit_group_id5 int, activiteit_id1 int, activiteit_id2 int, activiteit_id3 int, activiteit_id4 int, activiteit_id5 int, activiteit_omschrijving1 string, sic_group_id1 int, sic_group_id2 int, sic_group_id3 int, sic_id1 int, sic_id2 int, sic_id3 int, activiteit_group_section_id int, vestigingsadres_provincie_id int, vestigingsadres_plaats_id int, vestigingsadres_postcode_id int, vestigingsadres_plaats_letter int, vestigingsadres_gemeente_letter int, vestigingsadres_corop_gebied_id int, vestigingsadres_gemeente string, vestigingsadres_postcode_duizendtal int, vestigingsadres_postcode_hondertal int, vestigingsadres_postcode_tiental int, vestigingsadres_postcode string, personeel_id int, hoofdzaakfiliaalindicatie string, rechtsvorm_id int, rechtsvorm_categorie_id int, indimport string, indexport string, indicatie_faillissement string, indicatie_sursvanbet string, indicatie_economisch_actief string, achternaam string, vasttelefoonnr string, domeinnaam string, datum_oprichting int, datum_oprstichtingveren int, datum_vestiging int, datum_adres int, datum_voortzetting int, datum_opheffing int, datum_surseance int, datum_faillissement int, datum_mutatie int, datum_inschrijving_starter int, datum_inschrijving_niet_starter int, datum_oprichting_samengesteld int, indicatie_ultimate_moeder string, aantal_filialen_omvang_id int, concern_awp_klasse_omvang_id int, indicatie_actief string, indicatie_starter tinyint, bag_oppervlakte int, bag_gebouwoppervlaktebereik_id int, bag_gebruikersdoel int, bag_bouwjaar1leeftijd int, bag_gebouwbouwjaarbereik_id int, bag_count_bedrijven_1_adres int, bag_gebouwaantalopadresbereik_id int, edsn_gas string, edsn_elektra string, edsn_verbruik_elektra string, edsn_verbruik_gas string, edsn_grootverbruiker_elektra string, edsn_grootverbruiker_gas string, edsn_gas_aansluitingaantalbedrijven int, edsn_gas_aansluitingaantalbedrijvenbereik_id int, edsn_elektra_aansluitingaantalbedrijven int, edsn_elektra_aansluitingaantalbedrijvenbereik_id int, edsn_kv_gas_aantal int, edsn_kv_gas_bereik_id int, edsn_kv_el_aantal int, edsn_kv_el_bereik_id int, edsn_gv_gas_aantal int, edsn_gv_gas_bereik_id int, edsn_gv_el_aantal int, edsn_gv_el_bereik_id int, employee_total_omvang_bereik_id int, dnb_employees_total string, dnb_annual_sales_local_omzet_bereik_id int, dnb_annual_sales_local string, dnb_global_number_of_concernrelations string, dnb_global_number_of_concernrelations_id int, dnb_global_employee_total string, dnb_global_employee_total_omvang_bereik_id int, dnb_indicator_bevoegd string, dnb_du_indicator string, dnb_risk_indicator string, dnb_risk_indicator2 string, dnb_trading_indicator string, mobiel_of_vasttelefoonnr string, dnb_fax_nummer string, dnb_user_area string, contactbeherendvennoot int, contactverkoopmanager int, contactvennoot int, contactfirmant int, contactbevoegdfunctionaris int, contacteigenaar int, contactmarketingmanager int, contactfinancieelmanager int, contacthrmanager int, contactinkoopmanager int, contactitmanager int, contactdirecteur int, contactfleetmanager int, contactfacilitymanager int, location_lat decimal(18,14), location_lon decimal(18,14), lat decimal(18,14), lon decimal(18,14), gebouw_eigendom_id int, gebouw_verhuizing_id int, werkplek_aantal_laptop_bereik_id int, werkplek_aantal_pc_bereik_id int, werkplek_aantal_server_bereik_id int, bedrijfterrein_id int, bedrijfterrein_indicator tinyint, ziggo_indicator_coax tinyint, ziggo_indicator_footprint tinyint, anwb_aantal_voertuigen int, web_url string, web_rootcompany tinyint, web_url_manipulated string, search_field_all string, search_field_companyname string, search_field_address string, street_number string, street string, street_number_addition string, zipcode string, city string, phonenumber string, gdf_powerbereiken_id int, gdf_gasbereiken_id int, bag_identificatie string, rechtspersoon tinyint, nonmailing_indicator tinyint, web_url_match string, concernvoertuigpersoon int, concernvoertuigbestel int, concernvoertuigtruck int, concernvoertuigtrailers int, indicatie_global_ultimate_local string, global_ultimate_continent int, global_ultimate_country int, global_ultimate_country_character_1 int, global_ultimate_country_character_2 int, charging_station_distance int, charging_station_distance_id int, energylabel string, dnb_gu_indicator string, publication_id int,publication_coc_nummer_8 string, publication_publishing_date string, publication_publishing_date_int int, publication_publishing_text string, publication_publication_category_id int, publication_coc_region string, webpage_id int, webpage_hostname string, webpage_page_uri string, webpage_title string, webpage_anchor_text string, webpage_page_text string"
    val schema2 = "id,_id,kvknummer8,KvKnummer,company_all,address_only,company_only,duns,Zaaknaam,Handelsnaam,Vennootschapsnaam,plaatsnaam,hierarchytree_id,adres,status,concern_organisatie_type,vsivestigings_status_indicator,DNB_DUNS,postcode_id,concernlevel,sbi_id,awp,activiteit_group_id1,activiteit_group_id2,activiteit_group_id3,activiteit_group_id4,activiteit_group_id5,activiteit_id1,activiteit_id2,activiteit_id3,activiteit_id4,activiteit_id5,activiteit_omschrijving1,sic_group_id1,sic_group_id2,sic_group_id3,sic_id1,sic_id2,sic_id3,activiteit_group_section_id,vestigingsadres_provincie_id,vestigingsadres_plaats_id,vestigingsadres_postcode_id,vestigingsadres_plaats_letter,vestigingsadres_gemeente_letter,vestigingsadres_corop_gebied_id,vestigingsadres_gemeente,vestigingsadres_postcode_duizendtal,vestigingsadres_postcode_hondertal,vestigingsadres_postcode_tiental,vestigingsadres_postcode,personeel_id,hoofdzaakfiliaalindicatie,rechtsvorm_id,rechtsvorm_categorie_id,indimport,indexport,indicatie_faillissement,indicatie_sursvanbet,indicatie_economisch_actief,achternaam,vasttelefoonnr,domeinnaam,datum_oprichting,datum_oprstichtingveren,datum_vestiging,datum_adres,datum_voortzetting,datum_opheffing,datum_surseance,datum_faillissement,datum_mutatie,datum_inschrijving_starter,datum_inschrijving_niet_starter,datum_oprichting_samengesteld,indicatie_ultimate_moeder,aantal_filialen_omvang_id,concern_awp_klasse_omvang_id,indicatie_actief,indicatie_starter,bag_oppervlakte,bag_gebouwoppervlaktebereik_id,bag_gebruikersdoel,bag_bouwjaar1leeftijd,bag_gebouwbouwjaarbereik_id,bag_count_bedrijven_1_adres,bag_gebouwaantalopadresbereik_id,edsn_gas,edsn_elektra,edsn_verbruik_elektra,edsn_verbruik_gas,edsn_grootverbruiker_elektra,edsn_grootverbruiker_gas,edsn_gas_aansluitingaantalbedrijven,edsn_gas_aansluitingaantalbedrijvenbereik_id,edsn_elektra_aansluitingaantalbedrijven,edsn_elektra_aansluitingaantalbedrijvenbereik_id,edsn_kv_gas_aantal,edsn_kv_gas_bereik_id,edsn_kv_el_aantal,edsn_kv_el_bereik_id,edsn_gv_gas_aantal,edsn_gv_gas_bereik_id,edsn_gv_el_aantal,edsn_gv_el_bereik_id,employee_total_omvang_bereik_id,dnb_employees_total,dnb_annual_sales_local_omzet_bereik_id,dnb_annual_sales_local,dnb_global_number_of_concernrelations,dnb_global_number_of_concernrelations_id,dnb_global_employee_total,dnb_global_employee_total_omvang_bereik_id,dnb_indicator_bevoegd,dnb_du_indicator,dnb_risk_indicator,dnb_risk_indicator2,dnb_trading_indicator,mobiel_of_vasttelefoonnr,dnb_fax_nummer,dnb_user_area,contactbeherendvennoot,contactverkoopmanager,contactvennoot,contactfirmant,contactbevoegdfunctionaris,contacteigenaar,contactmarketingmanager,contactfinancieelmanager,contacthrmanager,contactinkoopmanager,contactitmanager,contactdirecteur,contactfleetmanager,contactfacilitymanager,location.lat,location.lon,lat,lon,gebouw_eigendom_id,gebouw_verhuizing_id,werkplek_aantal_laptop_bereik_id,werkplek_aantal_pc_bereik_id,werkplek_aantal_server_bereik_id,bedrijfterrein_id,bedrijfterrein_indicator,ziggo_indicator_coax,ziggo_indicator_footprint,anwb_aantal_voertuigen,web_url,web_rootcompany,web_url_manipulated,search_field_all,search_field_companyname,search_field_address,street_number,street,street_number_addition,zipcode,city,phonenumber,gdf_powerbereiken_id,gdf_gasbereiken_id,bag_identificatie,rechtspersoon,nonmailing_indicator,web_url_match,concernvoertuigpersoon,concernvoertuigbestel,concernvoertuigtruck,concernvoertuigtrailers,indicatie_global_ultimate_local,global_ultimate_continent,global_ultimate_country,global_ultimate_country_character_1,global_ultimate_country_character_2,charging_station_distance,charging_station_distance_id,energylabel,dnb_gu_indicator,webpage_id,webpage_hostname,webpage_page_uri,webpage_title,webpage_anchor_text,webpage_page_text"
    val l1 = schemaInit.split(",").map(_.trim.split(" ")(0)).toSet.toList.sorted
    val l2 = schema2.split(",").toSet.toList.sorted

//    l1 foreach println

    (l1.diff(l2)).foreach(println)
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