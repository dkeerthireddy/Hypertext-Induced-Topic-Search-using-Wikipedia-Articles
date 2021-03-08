import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._



object Hits {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hits").setMaster("spark://lincoln:31266")
    val sc = new SparkContext(conf)
    val titlefile = sc.textFile("hdfs://jefferson-city:31251/dataLoc/titles-sorted.txt")
    val indexedtitle= titlefile.zipWithIndex().map{case (k,v) =>((v+1).toInt, k)}
      .partitionBy(new HashPartitioner(5)).persist()
    val linksfile = sc.textFile("hdfs://jefferson-city:31251/dataLoc/links-simple-sorted.txt")
    val links = linksfile.map(x => (x.split(":")(0).toInt, x.split(":")(1)))
      .partitionBy(new HashPartitioner(5)).persist()
	
	//rootset
    val rootSet = indexedtitle.filter(f=> f._2.toLowerCase.contains(args(0).toLowerCase))
    rootSet.saveAsTextFile("hdfs://jefferson-city:31251/dataLoc/rootset")

    //base set
    //creating the inwards and outward link

    val outlinks = links.flatMapValues(y=> y.trim.split(" +")).mapValues(x=>x.toInt)
   
    //from the node
    val outgoing_links = outlinks.join(rootSet).map{case(k,(v1,v2)) => (k,v1)}
    

    //towards node
    val incoming_links = outlinks.map{case(k,v) => (v,k)}.join(rootSet).map{case(k,(v1,v2)) => (v1,k)}
   
    //union
    val base_links = outgoing_links.union(incoming_links).distinct().sortByKey().persist()
   

    var base_set = outgoing_links.map{case(k,v) => (v,k)}.union(incoming_links).groupByKey().join(indexedtitle).map{case(k,(v1,v2)) => (k,v2)}
    base_set = base_set.union(rootSet).distinct().sortByKey().persist()

    //initializing hub and auth scores
    var authscore = base_set.map{case(k,v) => (k, 1.0)}
    var hubscore = base_set.map{case(k,v) => (k, 1.0)}


    // iterating until maximum iterations 25 else converge

    val max_iterations = 25
    val count = 0
    breakable {
      for (count <- 0 to max_iterations) {
        // compute auth score
        val temp_auths = authscore

        authscore = base_links.join(hubscore).map { case (k, (v1, v2)) => (v1, v2) }.reduceByKey((x, y) => x + y).rightOuterJoin(hubscore).map { case (k, (Some(v1), v2)) => (k, v1); case (k, (None, v2)) => (k, 0) }
        val auths_sum = authscore.map(_._2).sum()
        authscore = authscore.map { case (k, v) => (k, (v / auths_sum).toDouble) }.persist()
        val auth_diff = temp_auths.join(authscore).map{case(v,k)=>(v,(k._1-k._2).abs)}
        val auth_diff_sum = auth_diff.map(_._2).sum()

        // compute hub score
        val temp_hubs = hubscore

        hubscore = base_links.map { case (k, v) => (v, k) }.join(authscore).map { case (k, (v1, v2)) => (v1, v2) }.reduceByKey((x, y) => x + y).rightOuterJoin(authscore).map { case (k, (Some(v1), v2)) => (k, v1); case (k, (None, v2)) => (k, 0) }
        val hubs_sum = hubscore.map(_._2).sum()
        hubscore = hubscore.map { case (k, v) => (k, (v / hubs_sum).toDouble) }.persist()
        val hub_diff = temp_hubs.join(hubscore).map{case(v,k)=>(v,(k._1-k._2).abs)}
        val hub_diff_sum = hub_diff.map(_._2).sum()
       
        //converge
        if(auth_diff_sum < 0.1 && hub_diff_sum  < 0.1)
          {
            break
          }
      }
    }

    var auths_output = authscore.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)

    var hubs_output = hubscore.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)

    //output

    
    base_set.coalesce(1).saveAsTextFile("hdfs://jefferson-city:31251/dataLoc/base")
    sc.parallelize(auths_output.take(50)).coalesce(1).saveAsTextFile("hdfs://jefferson-city:31251/dataLoc/auth")
    sc.parallelize(hubs_output.take(50)).coalesce(1)saveAsTextFile("hdfs://jefferson-city:31251/dataLoc/hubs")
  

  }



}
