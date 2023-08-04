package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  //An alternative way of calculating the neighbors for each cell defined inside HotcellUtils but results in a wrong answer
  /*spark.udf.register("countNeighbors",(x: Int, y: Int, z: Int)=>((
    HotcellUtils.countNeighbors(x,y,z, minX, maxX, minY, maxY, minZ, maxZ)
    )))*/

  pickupInfo.createOrReplaceTempView("pickup")

  //Count the number of pickups in each cell and store it in a column called 'attribute'
  pickupInfo = spark.sql("""SELECT x,y,z, COUNT(*) AS attribute FROM pickup   
                          WHERE x >= """+ minX.toString+ """ AND x <= """+ maxX.toString + """ AND y >= """+ minY.toString + """ AND y <= """+ maxY.toString +""" AND z >= """+ minZ.toString + """ AND z <= """+ maxZ.toString +
                        """ GROUP BY x,y,z """)
  pickupInfo.show()
  pickupInfo.createOrReplaceTempView("pickupattr")

  //Count the number of neighbors and the sum of their attributes for each cell and store them in neighbor_count and neighbor_attr_sum respectively
  var attributeInfo = spark.sql("""SELECT p1.x as x, p1.y as y, p1.z as z, sum(p2.attribute) as neighbor_attr_sum, count(*) as neighbor_count
                                 FROM pickupattr p1, pickupattr p2
                                 WHERE  abs(p1.x - p2.x) <= 1 AND abs(p1.y - p2.y) <= 1 AND abs(p1.z - p2.z) <= 1  
                                 GROUP BY p1.x, p1.y, p1.z """)


  
  attributeInfo.show()
  attributeInfo.createOrReplaceTempView("neighbors")



  var attr_sum = spark.sql("SELECT SUM(attribute) FROM pickupattr")
  var attr_sqrd_sum = spark.sql("SELECT SUM(attribute * attribute) FROM pickupattr")
  var x_bar = attr_sum.head().getLong(0)/numCells
  var S = scala.math.pow((attr_sqrd_sum.head().getLong(0)/numCells - scala.math.pow(x_bar, 2.0)),0.5)

  //Calculate the g-score for each cell and store in a column called G 
  var gInfo = spark.sql("""SELECT n1.x AS x, n1.y as y, n1.z as z,
                           ((n1.neighbor_attr_sum - """ + x_bar.toString + """ * n1.neighbor_count)/(""" + S.toString + """ * SQRT((""" + numCells.toString + """ * n1.neighbor_count - n1.neighbor_count*n1.neighbor_count)/""" + (numCells-1).toString + """))) AS G
                           FROM neighbors n1""")

  gInfo.show()
  gInfo.createOrReplaceTempView("gTable")

  var finalResult = spark.sql("SELECT x,y,z FROM gTable ORDER BY G DESC")


  return finalResult // YOU NEED TO CHANGE THIS PART
}
}
//pickup table contains x,y,z
//pickupattr table contains x,y,z and the attribute associated with that x,y,z
//neighbors table consists of x,y,z, neighbor_count and neighbor_attr_sum
//gTable contains g values
//NOT(p1.x = p2.x AND p1.y = p2.y AND p1.z = p2.z) AND
/*var gInfo = spark.sql("""SELECT n1.x AS x, n1.y as y, n1.z as z,
                           ((n1.neighbor_attr_sum - """ + x_bar.toString + """ * n1.neighbor_count)/(""" + S.toString + """ * SQRT((""" + numCells.toString + """ * n1.neighbor_count - n1.neighbor_count*n1.neighbor_count)/""" + (numCells-1).toString + """))) AS G
                           FROM neighbors n1, neighbors n2
                           WHERE n1.x = n2.x AND n1.y = n2.y AND n1.z = n2.z""")*/
////spark.sql("SELECT * FROM pickupattr p1, pickupattr p2 WHERE NOT(p1.x = p2.x AND p1.y = p2.y AND p1.z = p2.z) AND abs(p1.x - p2.x) <= 1 AND abs(p1.y - p2.y) <= 1 AND abs(p1.z - p2.z) <= 1").show()