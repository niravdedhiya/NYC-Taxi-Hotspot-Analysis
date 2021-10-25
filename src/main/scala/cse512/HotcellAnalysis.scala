package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
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
  pickupInfo.createOrReplaceTempView("pickupInfoView")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
  pickupInfo.createOrReplaceTempView("CellValues")

  pickupInfo = spark.sql("select x, y, z, count(*) as hotCellsValues from CellValues group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("CellHotness")
  
  val sumOfCells = spark.sql("select sum(hotCellsValues) as sumOfHotCells from CellHotness")
  sumOfCells.createOrReplaceTempView("sumOfCells")
  
  val meanOfCells = (sumOfCells.first().getLong(0).toDouble / numCells.toDouble).toDouble
  spark.udf.register("square", (inputX: Int) => (((inputX*inputX).toDouble)))
  
  val TotalSquare = spark.sql("select sum(square(hotCellsValues)) as TotalSquare from CellHotness")
  TotalSquare.createOrReplaceTempView("TotalSquare")

  val standardDevOfCells = scala.math.sqrt(((TotalSquare.first().getDouble(0).toDouble / numCells.toDouble) - (meanOfCells.toDouble * meanOfCells.toDouble))).toDouble
  
  spark.udf.register("NeighbourCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calculateNeighbourCells(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
  
  val NeighbourCells = spark.sql("select NeighbourCells(sch1.x, sch1.y, sch1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as NeighbourCellCount,"
  		+ "sch1.x as x, sch1.y as y, sch1.z as z, "
  		+ "sum(sch2.hotCellsValues) as sumOfHotCells "
  		+ "from CellHotness as sch1, CellHotness as sch2 "
  		+ "where (sch2.x = sch1.x+1 or sch2.x = sch1.x or sch2.x = sch1.x-1) "
  		+ "and (sch2.y = sch1.y+1 or sch2.y = sch1.y or sch2.y = sch1.y-1) "
  		+ "and (sch2.z = sch1.z+1 or sch2.z = sch1.z or sch2.z = sch1.z-1) "
  		+ "group by sch1.z, sch1.y, sch1.x "
  		+ "order by sch1.z, sch1.y, sch1.x")
	NeighbourCells.createOrReplaceTempView("NeighbourCells")
    
  spark.udf.register("ZScore", (NeighbourCellCount: Int, sumOfHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, meanOfCells: Double, standardDevOfCells: Double) => ((HotcellUtils.calculatezscore(NeighbourCellCount, sumOfHotCells, numCells, x, y, z, meanOfCells, standardDevOfCells))))
    
  pickupInfo = spark.sql("select ZScore(NeighbourCellCount, sumOfHotCells, "+ numCells + ", x, y, z," + meanOfCells + ", " + standardDevOfCells + ") as getisOrdStatistic, x, y, z from NeighbourCells order by getisOrdStatistic desc");
  pickupInfo.createOrReplaceTempView("ZScore")
    
  pickupInfo = spark.sql("select x, y, z from ZScore")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")
  
  return pickupInfo
}
}
