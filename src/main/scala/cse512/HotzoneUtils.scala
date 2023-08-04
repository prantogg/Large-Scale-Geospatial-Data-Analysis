package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var isInside = false
    val rectangle = queryRectangle.split(",")
    val point = pointString.split(",")
    val x1 = rectangle(0).toDouble
    val y1 = rectangle(1).toDouble
    val x2 = rectangle(2).toDouble
    val y2 = rectangle(3).toDouble
    val x = point(0).toDouble
    val y = point(1).toDouble
    isInside = x>=x1 && x<=x2 && y>=y1 && y<=y2
    return isInside // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
