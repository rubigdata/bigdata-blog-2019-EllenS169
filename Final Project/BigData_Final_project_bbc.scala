
object Cells {
  import nl.surfsara.warcutils.WarcInputFormat;
  import org.jwat.warc.{WarcConstants, WarcRecord};
  import org.apache.hadoop.io.LongWritable;
  import org.apache.commons.lang.StringUtils;
  import org.apache.spark.SparkConf;
  import java.io.InputStreamReader;
  import java.io.IOException;
  import org.jsoup.Jsoup;
//  import org.apache.spark.{Logging, SparkConf};



  /* ... new cell ... */

  // Adapt the SparkConf to use Kryo and register the classes used through reset parameter lastChanges (a function)


def main(args: Array[String]) {
  reset( lastChanges= _.

        set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ).

        set( "spark.kryo.classesToRegister", 

            "org.apache.hadoop.io.LongWritable," +

            "org.jwat.warc.WarcRecord," +

            "org.jwat.warc.WarcHeader" )

        )

  /* ... new cell ... */

  // Checking that configuration was successfully adapted

  sc.getConf.toDebugString

  /* ... new cell ... */

  val warcfile = "/opt/docker/notebooks/data/bigdata/bbc.warc"  //removed .gz

  /* ... new cell ... */

  val warcf = sc.newAPIHadoopFile(

                warcfile,

                classOf[WarcInputFormat],               // InputFormat

                classOf[LongWritable],                  // Key

                classOf[WarcRecord]                     // Value

      )

  /* ... new cell ... */

  val warc = warcf.map{wr => wr}.cache()

  /* ... new cell ... */

  //Number of html files

  val nHTML = warc.count()

  /* ... new cell ... */

  // WarcRecords header type info

  warc.map{ wr => wr._2.header }.

  map{ h => (h.warcTypeIdx, h.warcTypeStr) }.take(10)


  /* ... new cell ... */

  // Get responses with their size

  warc.map{ wr => wr._2.header }.

       filter{ _.warcTypeIdx == 2 /* response */ }.

       map{ h => (h.warcTargetUriStr, h.contentLength, h.contentType.toString) }.collect

  /* ... new cell ... */

  // WarcRecords with responses that gave a 404:

  warc.map{ wr => wr._2 }.

       filter{ _.header.warcTypeIdx == 2 /* response */ }.

       filter{ _.getHttpHeader().statusCode == 404 }.

       map{ wr => wr.header.warcTargetUriStr}. collect() 

  /* ... new cell ... */

  // WarcRecords corresponding to HTML responses:

  warc.map{ wr => wr._2 }.

       filter{ _.header.warcTypeIdx == 2 /* response */ }.

       filter{ _.getHttpHeader().contentType.startsWith("text/html") }.

       map{ wr => (wr.header.warcTargetUriStr, wr.getHttpHeader().contentType) }. collect()

  /* ... new cell ... */

  def getContent(record: WarcRecord):String = {

    val cLen = record.header.contentLength.toInt

    //val cStream = record.getPayload.getInputStreamComplete()

    val cStream = record.getPayload.getInputStream()

    val content = new java.io.ByteArrayOutputStream();

  

    val buf = new Array[Byte](cLen)

    

    var nRead = cStream.read(buf)

    while (nRead != -1) {

      content.write(buf, 0, nRead)

      nRead = cStream.read(buf)

    }

  

    cStream.close()

    

    content.toString("UTF-8");

  }

  /* ... new cell ... */

  // Taking a substring to avoid messing up the rendering of results in the Notebook - would need proper handling

  val warcc = warcf.

    filter{ _._2.header.warcTypeIdx == 2 /* response */ }.

    filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.

    map{wr => (wr._2.header.warcTargetUriStr, StringUtils.substring(getContent(wr._2), 0,2000))}.cache() //increased range substring to 2000 from 256

  /* ... new cell ... */

  warcc.take(10)

  /* ... new cell ... */

  def HTML2Txt(content: String) = {

    try {

      Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")

    }

    catch {

      case e: Exception => throw new IOException("Caught exception processing input row ", e)

    }

  }

  /* ... new cell ... */

  val warcc = warcf.

    filter{ _._2.header.warcTypeIdx == 2 /* response */ }.

    filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.

    map{wr => ( wr._2.header.warcTargetUriStr, HTML2Txt(getContent(wr._2)) )}.cache()

  /* ... new cell ... */

  var content = warcc.map{ tt => (tt._1, StringUtils.substring(tt._2, 0, 2000).toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))} //changed from 128 to 2000

  content.cache()

  content.count

  /* ... new cell ... */

  //Web pages containing bbc

  content.filter(_._2.contains("bbc")).count

  /* ... new cell ... */

  //Pages with incomplete data

  content.filter(!_._2.contains("bbc")).take(10)

  /* ... new cell ... */

  //Web pages containing world

  content.filter(_._2.contains("world")).count

  /* ... new cell ... */

  //Web pages containing news

  content.filter(_._2.contains("news")).count

  /* ... new cell ... */

  //Web pages containg europe

  content.filter(_._2.contains("europe")).count

  /* ... new cell ... */

  //Checks whether some links occur multiple times

  var sortedUrls = content.map( ff => (ff._1,1)).reduceByKey(_+_).sortBy(x => - x._2)

  sortedUrls.cache()

  sortedUrls.take(100)

  /* ... new cell ... */

  sortedUrls.filter(_._1.contains("weather")).take(100)

  /* ... new cell ... */

  //Check similarity of equal urls

  var sameUrl = content.filter(_._1.equals("<https://www.bbc.com/weather>"))

  var pairs = sameUrl.cartesian(sameUrl)

  var intersections = pairs.map{case((a,b),(c,d)) => (b intersect d).length}.take(100)

  var diff = pairs.map{case((a,b),(c,d)) => (b diff d).length}.take(100)

  /* ... new cell ... */

  //Discard url

  var text = content.map( ff => ff._2)

  text.take(10)

  /* ... new cell ... */

  //split document into words and remove special caracters 

  var words = text.flatMap(_.split(" "))

  words = words.map(_.toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))

  words = words.filter(!_.equals(""))

  words.take(10)

  

  //count word occurences

  var wordCounts = words.map(w => (w,1)).reduceByKey(_+_)

  

  //sort them by occurence

  var sorted = wordCounts.sortBy(x => -x._2)

  sorted.take(100)

  /* ... new cell ... */

  //Occurences of 'the' in the substring

  sorted.cache()

  sorted.filter(_._1.equals("the")).collect

  /* ... new cell ... */

  //Occurences of 'europe' in the substring

  sorted.filter(_._1.equals("europe")).collect

  /* ... new cell ... */

  //Occurences of 'brexit' in the substring

  sorted.filter(_._1.equals("brexit")).collect

  /* ... new cell ... */

  //Occurences of 'uk' in the substring

  sorted.filter(_._1.equals("uk")).collect

  /* ... new cell ... */

  //Occurences of 'trump' in the substring

  sorted.filter(_._1.equals("trump")).collect

  /* ... new cell ... */
}
}
                  
