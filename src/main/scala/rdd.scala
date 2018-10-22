import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp

import org.apache.spark.sql.functions.{avg, split, stddev_pop}
import java.text.SimpleDateFormat
import collection.mutable._


object rdd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
   
   // Register class for Kryo.
    val conf = new SparkConf()
        .registerKryoClasses(
          Array(
            classOf[TopicCount], 
            classOf[scala.collection.mutable.TreeSet[_]],
            classOf[scala.collection.mutable.HashSet[_]],
            Class.forName("scala.math.LowPriorityOrderingImplicits$$anon$6"),
            Class.forName("scala.Predef$$anon$1"),
            classOf[scala.runtime.ObjectRef[_]],
            Class.forName("scala.collection.immutable.RedBlackTree$BlackTree"),
            Class.forName("scala.collection.immutable.RedBlackTree$RedTree"),
            Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
            Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
            Class.forName("scala.collection.immutable.Set$EmptySet$"),
            classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
            classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats]
          )
        )

    val spark = SparkSession
          .builder
          .appName("lab_2")
          // .config("spark.master", "local")
          .config(conf)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryo.referenceTracking", "false")
          .config("spark.kryoserializer.buffer", "1024k")
          .config("spark.kryo.unsafe", "true")                // Use unsafe IO based kryo serialization.
          .config("spark.kryo.registrationRequired", "true")
          .config("spark.default.parallelism", args(2))       // Use a 1/2/3/4 times default parallelism level.
          .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    
    val filePath = "s3://gdelt-open-data/v2/gkg/" + args(0) + ".gkg.csv"    // Read dataset on s3.
 
    // var filePath = "src/main/resources/" + args(0) + ".gkg.csv"          // Read dataset on local.

    val raw_data = sc.textFile(filePath)

    val startTime = System.nanoTime

    // Functions used for aggregateByKey().
    val initialSet = SortedSet.empty[TopicCount]
    val addToSet = (s: SortedSet[TopicCount], v: (String, Int)) => s += TopicCount(v._1, v._2)
    val mergePartitionSets = (p1: SortedSet[TopicCount], p2: SortedSet[TopicCount]) => p1 ++= p2

    // Main steps.
    val filterRDD = raw_data.map(_.split("\t", -1))   // Split the row to list of strings.
        .filter(_.length > 23)  // Discard the row of which the length is less than 23 to avoid unexpected errors.
        .map(x => (x(1), x(23)))  // Select the "DATE" and "AllNames" columns.
        .map {    // Remove the duplicates in "AllNames" value within each record and change the Timestamp type to Date type.
          case (x, y) => (
            // new Timestamp(new SimpleDateFormat("yyyyMMddHHmmss").parse(x).getTime).toString.split(" ")(0)
            x.substring(0, 4) + "-" + x.substring(4, 6) + "-" + x.substring(6, 8)   // It's a little bit faster approach.
            , removeDuplicates(y))
        }
        // Flat the key-value pair to match key to every element in the value set. 
        // The 1st entry is "DATE", the 2nd is "AllNames" and the 3rd is the number of occurances of each topic.
        // MODIFIED in flatMatch: 
        //    1. Reconstruct has been included in flatMatch, so no need for reconstruct the structure on the next step.
        //    2. Records with empty topic are filtered inside flatMatch, which decrease the iteration time through rdds.
        //       So no need for filtering records on the next step.
        .flatMap {case (x, y) => flatMatch(x, y)} 
        // .map {case (x, y, z) => ((x, y), z)}    // Reconstruct the structure.      NOTE: NOT use anymore.
        // .filter {case ((x, y), z) => y != ""}   // Filter record with empty topic. NOTE: NOT use anymore.

        .reduceByKey((x, y) => x + y)           // Computer the number of occurances of each topic every event.
        .map {case ((x, y), z) => (x, (y, z))}  // Reconstruct the structure.

        // .sortBy(_._2._2, ascending = false)     // Descend by number of occurances of each topic.  NOTE: NOT use anymore.
        // .groupByKey()                           // Group by "DATE".                                NOTE: NOT use anymore.
        /**
         * MODIFIED: sortBy and groupByKey are combined to aggregateByKey which do these two work simultaneously and 
         *           decrease the iteration time through rdds.
         */
        .aggregateByKey(initialSet)(addToSet, mergePartitionSets)
        .map{case (x, y) => (x, y.take(10))}
        .map{case (x, y) => (x, y.toArray)}

    val fields = Seq("date", "results")
    val df = filterRDD.toDF(fields: _*)

    if (args(1) == "aws") df.write.mode(SaveMode.Overwrite).json("s3://zhaoyin/output/json")
    else df.write.mode(SaveMode.Overwrite).json("./json")

    // val time1 = (stage1Time - startTime) / 1e9d
    // val time2 = (stage2Time - stage1Time) / 1e9d
    // val time3 = (System.nanoTime - stage2Time) / 1e9d
    val duration = (System.nanoTime - startTime) / 1e9d

    if (args(1) == "aws"){}// duration.toString.saveAsTextFile("s3://zhaoyin/output/")
    else {
      println("Computation time: " + duration.toString + " s.")
      // println("Stage1: " + time1.toString + "s.")
      // println("Stage2: " + time2.toString + "s.")
      // println("Stage3: " + time3.toString + "s.")
    }

    sc.stop()
  }

  /**
    * Remove duplicate topics
    * @param names  Original topics (not yet split into list of topics)
    * @return   Set containing all different topics
    */
  def removeDuplicates (names: String): HashSet[String] = {

    val nameArray = names.split(";")
    val nameSet = HashSet[String]()

    for (name <- nameArray) {
      nameSet.add(name.split(",")(0))
    }
    nameSet
  }

  // Used for json format.
  case class nameCount(topic: String, count: Int)
  case class Results(date: String, result: List[nameCount])

  /**
    * Flat the key-value pair to match key to every element in the value set.
    * The 1st entry is "DATE", the 2nd is "AllNames" and the 3rd is the number of occurances of each topic.
    * MODIFED: Records with empty topics are filterd and ((x, y), z) structure is constructed here.
    * @param day  "DATE"
    * @param nameSet  Set containing different topics in one event.
    * @return   Set("DATE", "AllNames", Number)
    */
  def flatMatch (day: String, nameSet: HashSet[String]): HashSet[Tuple2[Tuple2[String, String], Int]] = {

    val nameTuple = HashSet[Tuple2[Tuple2[String, String], Int]]()

    for (name <- nameSet) {
      // Records with empty topics are filterd.
      if (name != "") {
        // Directly construct a ((x, y), z) structure.
        nameTuple.add(((day, name), 1))
      }
    }
    nameTuple
  }

  /**
   * Case class wraps "topic" and "count" and implements an ordering based on "count" which is used for SortedSet.
   */
  case class TopicCount(topic: String, count: Int) extends Ordered[TopicCount] {
    override def compare(that: TopicCount): Int =
      // Descending order.
      Ordering[Int].compare(that.count, this.count)
  }
}
