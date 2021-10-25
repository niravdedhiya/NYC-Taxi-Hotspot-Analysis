package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    var rectangle_x1 = rectangle(0).trim.toDouble
    var rectangle_y1 = rectangle(1).trim.toDouble
    var rectangle_x2 = rectangle(2).trim.toDouble
    var rectangle_y2 = rectangle(3).trim.toDouble
      
    var point = new Array[String](2)
    point= pointString.split(",")          
    var point_x=point(0).trim.toDouble
    var point_y=point(1).trim.toDouble
    
    var low_x = math.min(rectangle_x1, rectangle_x2)
    var high_x = math.max(rectangle_x1, rectangle_x2)

    var low_y = math.min(rectangle_y1, rectangle_y2)
    var high_y = math.max(rectangle_y1, rectangle_y2)
    
    if(point_y > high_y || point_x < low_x || point_x > high_x || point_y < low_y){
      return false
    }
    else{
      return true
    }


  }
}
