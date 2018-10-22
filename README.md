# Supercomputing for Big Data : Lab 2 Blog
Authors : Zhao Yin & Raphael Eloi (Group 30)
## Introduction
As part of the SuperComputing for Big Data class, we had to work during this quarter with the [Gdelt 2.0 Global Knownledge Graph](https://blog.gdeltproject.org/introducing-gkg-2-0-the-next-generation-of-the-gdelt-global-knowledge-graph/) dataset. The objective is to compute the top 10 most talked topics per day.

Firstly, we used the [Apache Spark](https://spark.apache.org/) library to resolve the program locally on our own computer and for very small fraction of the dataset. This was the objective of the first lab. After that, we used the [Amazon Web Services](https://aws.amazon.com/) to deploy the program on it and scale it to the whole dataset of several terabytes.

We are here interested in what we have learned doing those labs and the way we had to adapt and optimise our code to scale it to the whole dataset.

Apache Spark proposes us two different API's : Resilient Distributed Dataset (RDD's) and Spark SQL DataFrame/Datasets. We decided to consider both of them and use them to compute the whole dataset, so we could made comparison between them. Of course, since Dataframes are a more recent technology, they should be more efficient.

## Resilient Distributed Dataset
In lab 2, we made some code modifications and spark configurations to improve the RDD performance.

### Replace `groupyBykey()` with `aggregateByKey()`
In the code of lab 1, the last step is to group all the data based on the `"DATE"` key using `groupBykey()`. But `groupByKey()` is time consuming that all the key-value pairs will be shuffled around while using `aggregateByKey()` pairs on the same partitions with the same key are combined before the data is shuffled. So `aggregateByKey()` not only reduces the shuffled data, which solved the out of memory exception, but also speeds up the grouping process. More details are discussed in the next section.

### Do sort in `aggregateByKey()`
As we want to get the top 10 topics, a sort step is necessary. In lab 1, we used `sortBy()` to sort the RDDs by the `"count"` regardless of their `"DATE"` key. That is, RDDs with different `"DATE"` are combined together with `"count"` descending. Then in the following step, these RDDs are grouped by `"DATE"`. So, in lab 2, we came up with the idea that why not combine these two steps (sort and group) into a single one, which could reduce the number of times the RDDs are accessed. We detail the implementation below :

1. We define a case class `TopicCount` to wrap `"topic"` and `"count"` and extend it with `Ordered` class so different `TopicCount` objects could be sorted by their `count` attribute.
```scala
case class TopicCount(topic: String, count: Int) extends Ordered[TopicCount] {
    override def compare(that: TopicCount): Int =
        // Descending order.
        Ordering[Int].compare(that.count, this.count)
}
```
2. We use `Sortedset` to store these `TopicCount` objects which could automatically keeps their order. Three parameters and lambda functions are passed into `aggregateByKey()` :
```scala
val initialSet = SortedSet.empty[TopicCount]
val addToSet = (s: SortedSet[TopicCount], v: (String, Int)) => s += TopicCount(v._1, v._2)
val mergePartitionSets = (p1: SortedSet[TopicCount], p2: SortedSet[TopicCount]) => p1 ++= p2
```
3. The final code on `aggregateByKey()` :
```scala
.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
```

In this way, RDDs could be aggregated and sorted simultaneouly. The performace of this approach improves about 30% compared to the previous version in lab 1.

### More changes on the code
In lab 1, we have lines of code listed below :
```scala
// Here x is "DATE" and y is a set containing associated "topic"s.
// NOTE: One (x, y) represents a single event in the dataset, not a combination of events with a same "DATE".
.flatMap {case (x, y) => flatMatch(x, y)}
// Reconstruct the structure.
.map {case (x, y, z) => ((x, y), z)}
// Filter record with empty topic.
.filter {case ((x, y), z) => y != ""}
```
The `flatMatch()` method flats the key-value (`"DATE"` - set of `"topic"s`) pair to match the key (`"DATE"`) to every element (`"topic"`) in the value set (`"topic"`s) and outputs (`"DATE"`, `"topic"`, `"count"`). After this step, we used `map()` to reconstruct the rdd structure from (`"DATE"`, `"topic"`, `"count"`) to ((`"DATE"`, `"topic"`), `"count"`) and then filtered records with empty `"topic"`. 

But in lab 2, we find that this approach is pretty stupid and time consuming. So what we have improved is that `flatMatch()` directly outputs ((`"DATE"`, `"topic"`), `"count"`) avoiding a `map()` operation and at the same time discards the record with empty `"topic"`. We put the modified `flatMatch()` here :
```scala
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
```
Though small changes, they bring performance improvement of 10% which I think is because a unecessary `map()` has to once again iterate through the whole RDDs and the same with another unecessary `filter()` operation. So it's not surprising to see the speedup.


### Kryo Serializer
In lab 2, we utilize a significantly faster and more compact serializer, Kryo serializer compared to Java serializer, to do the data serialization work. Here are the classes we registered for Kryo classes :
```scala
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
```
Then we set some Kryo configurations as below :
```scala
.config(conf)
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
.config("spark.kryo.referenceTracking", "false")
.config("spark.kryo.registrationRequired", "true")
```
After changing the serializer, however, we didn't see any performance improvement which we think is quite abnormal. It should work somehow. So we enlarged the initial kryo serializer buffer to 1024k and used unsafe IO based Kryo serialization : 
```scala
.config("spark.kryoserializer.buffer", "1024k")
.config("spark.kryo.unsafe", "true") // Use unsafe IO based kryo serialization.
```
As always, no improvent observed. After searching, we saw [offical spark website](https://spark.apache.org/docs/latest/tuning.html#data-serialization) puts: 
> Formats that are slow to serialize objects into, or consume a large number of bytes, will greatly slow down the computation.

which indicates that obvious improvement will be observed if the application contains many large objects. But for our application, the largest object, `SortedSet` object which contains many `TopicCount` objects, is not large enough for Kryo serializer to play its role.

### Spark parallelism level
From [spark website](https://spark.apache.org/docs/latest/tuning.html#level-of-parallelism) :
> Clusters will not be fully utilized unless you set the level of parallelism for each operation high enough.

Spark parallelism level affect the number of tasks assigned to each node during operations of `groupBykey()`, `reduceByKey()`, `aggregateByKey()` and etc. Therefore, we explored this config to see if there could be any improvement by setting level to 1/2/3/4 times of the total number of cores on all executor nodes (1 time is the default value). An up to 5% improvement is observed in our experiments which we think the reason is (from [spark](https://spark.apache.org/docs/latest/tuning.html#memory-usage-of-reduce-tasks))
> Spark can efficiently support tasks as short as 200 ms, because it reuses one executor JVM across many tasks and it has a low task launching cost, so you can safely increase the level of parallelism to more than the number of cores in your clusters.

Therefore, small tasks make one executor JVM reused which speeds up the whole application.


## Dataframes
The main difference between the RDD's and the dataframes, is that the dataframes use structured data. During the implementation, we define the initial schema of the data and then we work on the column names.

First, we read the data from the csv files stocked on the Amazon S3 bucket of the GDelt GKG dataset and thus import the dataset of 3,7 terabytes wich is quiet consequent. Once, the data is imported, we are going to work on the column by deleting, exploding, counting, ... The resulting dataframe is a subset having the following schema : 
```scala
root
 |-- date : Date (nullable = false)
 |-- topic : String (nullable = false)
 |-- count : Int (nullable = false)
```
One record of this subset indicates that a certain topic appears in a "count" number of sources at some date. We can rank the records w.r.t the "count" column and grouped by date. Finally, we apply a filter to keep only the top 10 topics per day. 

This could be the final solution. However, we need to write the results into a json file and in order to write it into the expected format, we need to change the schema into this schema : 
```scala
root
 |-- data : Date (nullable = false)
 |-- result : Struct (nullable = false)
 |   |-- topic : String (nullable= false)
 |   |-- count : Int (nullable = false)
 ```
 
## Cluster configuration
Apart from code modifications and spark configurations, there's a cluster configuration that really troubled us for some time. It's `"maximumRessourceAllocation"` config which defauls as `false`. 

At first, we run the application on EMR with 2 nodes and a small part of dataset and it works as we expected. But when we scaled it on whole dataset or just on one year of dataset, an out of memory exception was always returned. But from Ganglia, we clearly knew that the whole memory of all nodes still contained large free space which should be fully utilized. 

After we set `"maximumRessourceAllocation"` to `true`, problem solved and everything works as expected because it will use the maximum possible resources on each node of the cluster.

## Performance
Finally, our implementation has a performace of 10 minutes for RDD and 8 minutes for Dataframe on whole dataset.
