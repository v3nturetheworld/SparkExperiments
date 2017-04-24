// Created by Paul Heinen 4/23/2017
// pheinenjr@gmail.com


import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Lab4 {
  // Equivalent in Java to public static void main(String[] args)
  def main(args: Array[String]) {

    if (args.length != 4) {
      println("Invalid number of arguments. Usage: Lab4 master input output source")
      throw new IllegalArgumentException("Invalid num of arguments. You Entered: " + args.length + ". Need 4")
    }
    // Setup Spark Configuration for this application
    val scfg = new SparkConf().setAppName("Lab4 Playing With GraphX").setMaster(args(0))
    // link spark context
    val sc = new SparkContext(scfg)

    //Open the file. Notice how we access arrays differently. We use the () operator
    // rather then the [] operator like you'd find in Java
    val file = sc.textFile(args(1))

    // OK. This is where things stray far far away from Java 7 at least. If you've played around with Java 8
    // or functional programming, things will be familiar.
    // val edgesRDD: RDD[Edge[BigInt]] is defining an RDD type. This already assumes
    // that we will supply two vertices, but the parameter were actually giving Edge defines
    // the property of the Edge. In our case, this property is a weight of type BigInteger (Java type)
    val edgesRDD: RDD[Edge[Long]] = file.map(line => line.split(","))
      //This maps the values to an Edge and converts the input lines which are treated as
      // strings to Long value types which GraphX requires.
      .map(line => Edge[Long](line(0).toLong, line(1).toLong, line(2).toLong))

    val source: VertexId = args(3).toLong // Source Vertex to calculate shortest path from

    //Now that we have all our edges, we can easily compose our graph using the Graph.fromEdges function.
    // We give specified source vertex a weight of 0 and the other vertices a huge weight
    val graph: Graph[Long, Long] = Graph.fromEdges(edgesRDD, 0).mapVertices((vid, _) =>
      if (vid == source) 0 else Long.MaxValue)

    //We now have our graph. This could have been done in even less code (like 1 line)

    //Print the vertices
    def printVertices(g: Graph[Long, Long]) = g.vertices.collect().foreach(println(_))

    //print the edges
    def printEdges(g: Graph[Long, Long]) = g.edges.collect().foreach(println(_))

    // Helper function for comparing
    def compare(a: Long, b: Long) = {
      //println("a: " + a + ", b: " + b)
      if(a < b) a else b
    }
    // Now we can compute the Shortest Path using Pragel. Please reference http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.GraphOps@pregel[A](initialMsg:A,maxIterations:Int,activeDirection:org.apache.spark.graphx.EdgeDirection)(vprog:(org.apache.spark.graphx.VertexId,VD,A)=%3EVD,sendMsg:org.apache.spark.graphx.EdgeTriplet[VD,ED]=%3EIterator[(org.apache.spark.graphx.VertexId,A)],mergeMsg:(A,A)=%3EA)(implicitevidence$6:scala.reflect.ClassTag[A]):org.apache.spark.graphx.Graph[VD,ED]
    // for the next part.
    val shortest_path = graph.pregel(Long.MaxValue)(
      (vid, distance, delta_distance) => compare(distance, delta_distance),
      // Define a tuple type, which is going to be a (V1, V2, E) tuple
      // srcAttr = V1, dstAttr = V2, attr = attribute = Long
      edge_tuple => {
          if(edge_tuple.srcAttr != Long.MaxValue && edge_tuple.attr != Long.MaxValue) {
            if (edge_tuple.srcAttr + edge_tuple.attr < edge_tuple.dstAttr) {
              Iterator((edge_tuple.dstId, edge_tuple.srcAttr + edge_tuple.attr))
            } else {
              // going down this path will not yield the shortest path
              Iterator.empty
            }
          } else {
            // Overflow safety check...
            println("Overflow will occur!!!!!!!!")
            Iterator.empty
          }
      },
      (pragel_msg1, pragel_msg2) => compare(pragel_msg1, pragel_msg2)
    )

    // Cool. Were done


    shortest_path.vertices.saveAsTextFile(args(2))


  }
}
