# Blogpost Map Reduce
In this blog post we are going to discuss the following two topics:
- The workings behind basic Hadoop commands
- Counting the number of words in *Shakespeare* using Map-Reduce


## Basic Hadoop Commands
| Command                            | Meaning                                                                     |
| -----------------------------------  | ------------------------------------------------------------------    |
| `sbin/start-dfs.sh`                 | Starting HDFS, this also includes NameNode, Secondary NameNode and DataNodes |
| `bin/hdfs dfs -mkdir /user`    | A new directory is created in the distributed file system |
| `bin/hdfs dfs -put etc/hadoop input` | The local folder *hadoop* is copied into the folder *input* in the distributed file system |
| `bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar grep input output 'dfs[a-z.]+'` | This command runs mapReduce on input using input and writes everything which matchs `[a-z.]+` to a file in the distributed directory output |
|`bin/hdfs dfs -get output output` | The folder output is copied from the dfs to the local docker file system | `bin/hdfs dfs -ls hdfs://localhost:9001/user/root/input` | All files in the distributed folder input are listed|
| `sbin/stop-dfs.sh` | Stopping the file system | 
| `bin/hadoop com.sun.tools.javac.Main WordCount.java` | This complies *WordCount.java* |
| `jar cf wc.jar WordCount*.class` | Creating a jar file of WordCount |
| `bin/hadoop jar wc.jar WordCount input output`| Running wordcount on *input* and saving it in *output*|

In the distributed file system (dfs) `hdfs dfs` many basic linux commands can be performed such as `-ls`, `-rm` and `-cat`. It is possible to inspect a file using `cat` in the distributed file system. However, this is not preferred, especially for larger output files. Unfortunately, commands like `nano` and `vim` do not work in the distributed file system. Therefore, to inspect the file using one of our preffered text editors we must copy the output directory from the distributed file system to the local docker file system. This is usually done using the `get` command. Other often used commands are `copyFromLocal` and respectively `copyToLocal`.
Many of this steps might seem very obvious or self-explanatory; however, I decided to include them for a better understanding for myself.
 
## Counting the number of words in *Shakespeare* using Map-Reduce
Using a practical example, I am going to try to explain the basic workings of Map-Reduce. 
The map function is called on every item of the input and extracts something of interest from each represented by intermediate key-value pairs. This means that in our case the map function is called on every word in the the shakespeare file /100.txt and it returns for each word the intermediate key-value pair i.e (word,one). 

```
 public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
```
After getting the intermediate keys from the mapper, a combiner could be used to simplify the work of the reducer by reducing the communication that are commutative and associative. For example when giving the key-value pairs ("the",1), ("the",1), ("Julia",1) to the combiner, the combiner could output ("the",2), ("Julia",1) to reduce the work of the reducer. However, no combiner is used in the wordcount example.

The next step is to run the reducer on the intermediate key-value pairs and reduce all values with the same key together. In general, all values with the same key are sent to the same reducer. This can have negative effects on the efficiency, since the reducer with all key-value pairs related to the key "the" will have a lot of workload, whereas the reducer with all key-values pairs with the key "zodiac" will only have little work. But that's just a small remark. 
In the shakespeare example, the reduce function sums up all the values of the key-value pairs with the same key and writes the key with the resulting sum to the file. The exact function can be seen below. 

```
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

```
After the reducer is done, we can inspect the output file to see whether the word Romeo or Juliet occurrs more often. When inspecting the file it is possible to see that there are multiple keys containing Juliet such as *Juliet.*, *Juliet!*. If we would like to have only one single count for Juliet, we could remove all punctuation marks from the words in the map function. Looking at the counts we can see that Romeo occurs 313 and Juliet only 206 times. Thus, Romeo occurs more often that Julia. This can be answered by making only a singlr pass over the corpus.
