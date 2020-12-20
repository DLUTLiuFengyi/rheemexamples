package com.github.dlut.pagerank.scala

import org.qcri.rheem.api.{PlanBuilder, _}
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.graphchi.GraphChi
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

/**
 * This is a Rheem implementation of the PageRank algorithm with some preprocessing.
 */
class PageRank(configuration: Configuration, plugins: Plugin*) {

  /**
   * Executes this instance.
   *
   * @param inputUrl      URL to the first RDF NT file
   * @param numIterations number of PageRank iterations to perform
   * @return the page ranks
   */
  def apply(inputUrl: String, numIterations: Int) = {
    // Initialize.
    val rheemCtx = new RheemContext(configuration)
    plugins.foreach(rheemCtx.register)
    implicit val planBuilder = new PlanBuilder(rheemCtx)
      .withJobName(s"PageRank ($inputUrl, $numIterations iterations)")
      .withUdfJarsOf(this.getClass)

    // Read and parse the input file.
    val edges = planBuilder
      .readTextFile(inputUrl).withName("Load file")
      .filter(!_.startsWith("#"), selectivity = 1.0).withName("Filter comments")
//      .map(PageRank.parseZhihuFansCase).withName("Parse Zhihu case")
      .map(PageRank.parseLiveJournalSocialCase).withName("Parse Live Journal case")
//      .map(PageRank.parseTriple).withName("Parse triples")
//      .map { case (s, p, o) => (s, o) }.withName("Discard predicate")

    // Create vertex IDs.
    val vertexIds = edges
      .flatMap(edge => Seq(edge._1, edge._2)).withName("Extract vertices")
      .distinct.withName("Distinct vertices")
      .zipWithId.withName("Add vertex IDs")

    // Encode the edges with the vertex IDs
    type VertexId = org.qcri.rheem.basic.data.Tuple2[Vertex, String]
    val idEdges = edges
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Join source vertex IDs")
      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }.withName("Set source vertex ID")
      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Join target vertex IDs")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0)).withName("Set target vertex ID")

    // Run the PageRank.
    // Note: org.qcri.rheem.api.graph._ must be imported for this to work.
    val pageRanks = idEdges.pageRank(numIterations)

    // Make the page ranks readable.
    pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join page ranks with vertex IDs")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1)).withName("Make page ranks readable")
      .collect()

  }


}

/**
 * Companion for [[PageRank]].
 */
object PageRank {

  def main(args: Array[String]) {
    // Parse args.
    if (args.isEmpty) {
      println("Usage: <main class> <plugin(,plugin)*> <input file> <#iterations>")
      sys.exit(1)
    }
    val begin = System.currentTimeMillis()

    val plugins = parsePlugins(args(0))
    val inputFile = args(1)
    val numIterations = args(2).toInt

    // Set up our wordcount app.
    val configuration = new Configuration
    val pageRank = new PageRank(configuration, plugins: _*)

    // Run the wordcount.
    val pageRanks = pageRank(inputFile, numIterations)
    val result = pageRanks.toSeq.sortBy(-_._2)

    val end = System.currentTimeMillis()
    println("Computation using time: " + (end - begin) + "ms")
    println(sumOfRanks(result))

    val file: File = new File(args(3))
    val fileWriter: FileWriter = new FileWriter(file, true)
    val out: BufferedWriter = new BufferedWriter(fileWriter)
    out.write( "Datasets: " + args(1) + "\n"
      + "Iteration times: " + args(2) + "\n"
      + "Computation using time: " + (end - begin) + "ms" + "\n")
    out.close()
    fileWriter.close()
  }

  /**
   * Parse a comma-separated list of plugins.
   *
   * @param arg the list
   * @return the [[Plugin]]s
   */
  def parsePlugins(arg: String) = arg.split(",").map {
    case "basic-graph" => RheemBasics.graphPlugin
    case "java" => Java.basicPlugin
    case "java-conversion" => Java.channelConversionPlugin
    case "java-graph" => Java.graphPlugin
    case "spark" => Spark.basicPlugin
    case "spark-graph" => Spark.graphPlugin
    case "graphchi" => GraphChi.plugin
    case other: String => sys.error(s"Unknown plugin: $other")
  }

  /**
   * Parse a NT file triple.
   *
   * @param raw a [[String]] that is expected to conform to the pattern `<subject> <predicate> <object>|<literal> .`
   * @return the parsed triple
   */
  def parseTriple(raw: String): (String, String, String) = {
    // Find the first two spaces: Odds are that these are separate subject, predicated and object.
    val firstSpacePos = raw.indexOf(' ')
    val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

    // Find the end position.
    var stopPos = raw.lastIndexOf('.')
    while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

    (raw.substring(0, firstSpacePos),
      raw.substring(firstSpacePos + 1, secondSpacePos),
      raw.substring(secondSpacePos + 1, stopPos))
  }

  /**
   * 解析知乎粉丝关注case的数据集
   * id, sourceNode, endNode, ...
   * 最后返回结果格式是 (sourceNode, endNode)
   * @param raw
   * @return
   */
  def parseZhihuFansCase(raw: String): (String, String) = {
    val firstSpacePos = raw.indexOf(',')
    val secondSpacePos = raw.indexOf(',', firstSpacePos + 1)
    val thirdSpacePos = raw.indexOf(',', secondSpacePos + 1)

    (
      raw.substring(firstSpacePos + 1, secondSpacePos),
      raw.substring(secondSpacePos + 1, thirdSpacePos))
  }

  /**
   * 解析Stanford 大规模复杂网络组提供的Live Journal Social数据集
   * @param raw
   * @return
   */
  def parseLiveJournalSocialCase(raw: String): (String, String) = {
    val items = raw.split("\t")

    (
      items(0),
      items(1))
  }

  /**
   * 计算每一次迭代里所有节点的page rank值之和
   * @param pageRanks
   * @return
   */
  def sumOfRanks(pageRanks: Iterable[(String, java.lang.Float)]): Float = {
    var rankSum = 0.0f
    pageRanks.foreach(pr => {
      rankSum  = rankSum + pr._2
    })
    rankSum
  }
}
