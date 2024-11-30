import org.apache.spark.SparkContext



val sparkContext =  // Your SparkContext instance

val listeners = sparkContext.getSparkListeners()



if (listeners.isEmpty) {

  println("No Spark listeners are registered")

} else {

  println("Registered Spark listeners: " + listeners)

}
