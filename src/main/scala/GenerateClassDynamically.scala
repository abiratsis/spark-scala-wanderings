
object GenerateClassDynamically {

  def generateClass(members: Map[String, String], name: String): Any = {
    val classMembers = for (m <- members) yield {
      s"${m._1}: String"
    }

    val classDef = """case class Person(name: String,phone: String,address :Map[String, String])extends IPerson;scala.reflect.classTag[Person].runtimeClass"""
    classDef
//    import scala.reflect.runtime.universe
//    import scala.tools.reflect.ToolBox
//    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
//
//    tb.compile(tb.parse(classDef))()
//      .asInstanceOf[Class[_]]
//      .getDeclaredConstructors()(0)
//      .newInstance(List.fill(members.values.size)(""): _*)
  }

  def main(args: Array[String]): Unit = {
    import org.codehaus.jackson.node.{ObjectNode, TextNode}
    import collection.JavaConversions._

    import rapture.json._
    import jsonBackends.jackson._

    val jsonSchema = """{"name":"Alex","phone":"2322321","address":{"street":"Lincoln", "number":"65", "postcode":"1121LN"}}"""
    val json = Json.parse(jsonSchema)

    val mapping = collection.mutable.Map[String, String]()
    val fields = json.$root.value.asInstanceOf[ObjectNode].getFields

    for (f <- fields) {
      (f.getKey, f.getValue) match {
        case (k: String, v: TextNode) => mapping(k) = v.asText
        case (k: String, v: ObjectNode) => v.getFields.foreach(f => mapping(f.getKey) = f.getValue.asText)
        case _ => None
      }
    }

    val dynClass = generateClass(mapping.toMap, "JsonRow")
    println(dynClass)
  }
}
