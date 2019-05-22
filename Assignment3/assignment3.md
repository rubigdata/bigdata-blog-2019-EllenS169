# Assignment 3A
This blog post will focus on the internals of Spark specifically looking into the details of query processing in Spark.
We will start with a small introduction on Spark, followed by when and when not you should use Spark and finishing with an insight into the Spark internals using Scala code examples.

## Introduction
Apache Spark is a unified analytics engine for large-scale data processing introduced by Matei Zahari in 2014.
Spark makes use of a new abstraction called  **Resilient Distributed Datasets** short **RDD**, which gives direct control of data sharing. RDDs are distributed collections of objects cached in memory across cluster nodes. Using RDDs has multiple advantages. 
Spark makes use of parallel operators to manipulate data in RDDs which decreases processing time. Furthermore, RDDs have a high fault-tolerance, as it is possible to automaticly reconstruct intermediate results upon failure by making use of a so called **lineage**. 

Spark operators can be split into two subclasses: *Transformations* and *Actions*. Transformations are lazy operations that define a new RDD for example a filtered RDD. Common transformation operators are `map` , `filter` , `sample` , `join`, `groupby` and so forth. These transformation are *coarse-grained*, as they apply the same operation to many data items. Coarse-grained transformations efficiently provide fault-tolerance, since there is no need to save the actual daa rather only logging the transformations is sufficient to recover intermediate results.  
The second type of operations are actions. Actions return a result to the driver program. Common actions are for example `collect`, `reduce`, `count`, `save` and `lookUpKey`.
RDDS also provide two additional 'features' to speed up processing: *partitioning* and *persistence*.The user can control partitioning to optimize data placement across queries. This is usually more efficient than the sort-based approach of MapReduce.
Furthermore, the user can control persistence by choosing different storage strategies for each RDD. For example, choosing in memory storage for an RDD which will be used by multiple other RDDs. 

## When and when not make use of Spark?

Two types of queries are very expensive to perform in Hadoop and other distributed file systems: *interactive analysis* and *iterative machine learning algorithms*. These types of queries are mainly slow due to replication, serialization and disk input output. A solution to this problem is provided by Spark. As learned in the previous section, Spark makes use of distributed memory to keep information in memory which might be used by multiple queries making it 10-100 times faster than getting the information from network and disk.

There are two main challenges related to distributed memory abstraction. The distributed memory needs to be fault-tolerant and efficient in large commodity clusters. Other distributed storage abstractions have offered an interface based on fine-grained updates (reads and writes to cells in a table). However, this is not well suited for Big Data as it requires replicating data or update logs across nodes for fault tolerance which is very expensive for data-intensive apps such as Big Data. Spark solves this elegantly and efficiently by making use of coarse-grained transformations and its lineage. Therefore, Spark is chosen by most Big Data companies as their preferred abstraction.
Even though Spark generally performs very well on several Big Data problems, there are also some problems for which it is less advised to use of Spark. According to Matein Zaharia RDDs are less suited for applications which make asynchronous fine-grained updates to a shared state. Examples for that would be incremental web crawlers or web applications.  

## Spark internals
I will try to explain the internal workings of Spark making use of the classic Big Data "Hello World" exercise - counting words. Therefore, we will make use of Shakespeare data to explain the internal workings of Spark line by line.

First a new RDD HadoopRDD will be created by getting the Shakespeare data from Hadoop. An RDD has multiple attributes: partitions, dependencies, preferredLocation and partitioner. In the case of the HadoopRDD the partitions correspond to the number of blocks, it does not have any dependencies, the preferredLocation is generally where the blocks are located on the machine and the partitioner is unknown.
```scala
val lines = sc.textFile("/opt/docker/hadoop-2.9.2/100.txt")
```
But what exactly happens when a new RDD object is created?
When an RDD object is created a directed acyclic graph (DAG)  is built. The DAG is then passed to the DAGScheduler which splits the graph into stages of tasks. The task set is then passed to the TaskScheduler, which then launches the task via the cluster manager. The task is then given to the worker and the executed and stored. In case a task failed (e.g. due to a straggling task) the TaskScheduler retries the tasks by informing the DAGScheduler again. If every task has been successfully performed the DAGScheduler then submits each stage as ready.
Now that we know more about what exactly happens when an RDD object is created we can move on to the next example.


```scala
println( "Lines:\t", lines.count, "\n" + 
         "Chars:\t", lines.map(s => s.length).
                           reduce((a, b) => a + b))

```

In the example the operation `count` gets called on the HadoopRDD created previously, since `count` is an action this will output how many lines the Shakespeare file contains. 
In the second call, first a transformation is called on the HadoopRDD creating a new MapRDD, and then the action reduce is called on the MapRDD. This makes explainig how Spark computes the result a bit more complex. When `map` is called on the HadoopRDD and MapRDD is created the transformation is not evaluated yet as Spark makes use of *lazy evaluation*. So when the action reduce is called on the MapRDD, to compute the result Spark needs to follow back the lineage. First it goes to the RDD the action is called on so in our case the MapRDD. Then, to be able to compute MapRDD it needs to go to the parent RDD of MapRDD which is the HadoopRDD.  The HadoopRDD does not have any parent RDDs so it can start to compute the result. Using the shakespeare data it can then compute the MapRDD and then having the MapRDD it can finally perform the `reduce` action and output the corresponding result - the number of characters. In other words to run a job all parent RDDs first need to be computed up to the RDD with no dependencies.


At this point it might be beneficial to first explain the concept of dependencies. 
There are two types of dependencies: *narrow* and *wide* dependencies. In narrow dependencies each partition of the parent RDD is used by at most one partition of the child RDD, whereas in wide dependencies one child partition may depend on multiple parent partitions. Generally, you want to decrease the number of wide dependencies and increase the number of narrow dependencies, since narrow dependencies allow for piplines execution on one cluster node while wide dependencies require data from all parent partitions to be available and shuffled across nodes. Decreasing the number of wide dependencies also results in higher efficiency especially when recovering after a node failure. 

Examples of operations with narrow dependencies are: `map`, `filter`, `join` (with inputs co-partioned) and examples of operation wit wide dependencies are: `groupBy` and `join` (with not co-partitioned inputs).

So for example, the use of filter in the code below results in narrow dependencies between the parent RDD FlatMapRDD and the resulting FilteredRDD.

```scala

val words = lines.flatMap(line => line.split(" "))
              .filter(_ != "")
              .map(word => (word,1))

```

Note: `flatMap` is used here instead of `map` since we want a map similar to the one in MapReduce in other words map is a one-to-one mapping, whereas flatMap maps each input value to one or more outputs. 
As you might have already guessed everything the code above does it first creating a new FlatMapRDD by calling flatMap on lines. This will transform the HadoopRDD into a FlatMapRDD by spliting each line dependening on spaces, basically mapping one line to all words in the line. Afterwards the FlatMapRDD will be transformed to a FilterRDD by filtering out all empty words. Finally, the FilteredRDD while be transformed to a MapRDD by mapping all words to key-value pairs. But remember that this will not output anything yet not will be evaluated/transformed yet since no action has been called. 
We can now perform our normal MapReduce job by adding up all values for pairs with the same key.

```scala
val wc = words.reduceByKey(_ + _)

```

Note again that this still won't output anything, since reduceByKey is also a transformation. So only by for example calling `wc.take(10)` every transformation made earlier will be evaluated and 10 words and their occurence will be outputed.

Let's say we would like to compute how often Romeo and Julia occurs in the Shakespeare data. We could do this by running the code below. 
```scala
wc.filter(_._1 == "Romeo").collect
wc.filter(_._1 == "Julia").collect
```
However, by doing so we would not make use of the main advantage of Spark - distributed memory. As said previously, it is very efficient to use spark for interactive queries so multiple queries on the same data, since we can keep the data in memory and do not need to recompute it. As we can see these are interactive queries, since are calling to queries on the same intermediate result. However, we do not save the intermediate result in memory. So by first calling:

```scala
wc.cache()
wc.filter(_._1 == "Romeo").collect
wc.filter(_._1 == "Julia").collect
```
We can significantly speed up the calcualtion of how often Romeo and Julia occurred, since we saved the intermediate result wc in cache.

If you are now to save the results of wc, you might be wondering why you don't only have one but in my case two text files. That is because the result is also partitioned into several blocks.

When counting the occurence of Romeo you might be wondering why Romeo only occurs 45 times. This is, since in the above example only exact occurence of "Romeo" are counted meaning that e.g. "Romeo!" does not count. Therefore, to know how often Romeo actually occured we can do the following:

```scala
val words = lines.flatMap(line => line.split(" "))
              .map(w => w.toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
              .filter(_ != "")
              .map(w => (w,1))
              .reduceByKey( _ + _ )
```

Contrary to the code above, here we first map all words to lower case and remove all special characters and numbers before reducing them.
This then results in the actual occurence of Romeo: 291.

As explained earlier performance can also be improved by proper partitioning. The default partitioner in Spark is none. To define your own partition you can do as follows:


```scala

val rddPairsPart2 = rddPairs.partitionBy(new HashPartitioner(2))

```

rddPairs is now partitioned into two partitions instead of the default partitions. When working with partitioners it is important to remember that generally partitioning is dependent on the distributed operations performed meaning that "only operations with guarantees about the output distribution will carry an existing partitioner over to its result" (Quote: from second notebook). 

I hope this gives you a good starting insight into the Spark internals. 




