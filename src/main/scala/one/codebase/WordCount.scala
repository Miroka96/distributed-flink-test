package one.codebase

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text sever (at port 12345)
 * using the ''netcat'' tool via
 * {{{
 * nc -l 12345
 * }}}
 * and run this example with the hostname and the port as arguments..
 */
object WordCount {

  /** Main program method */
  def main(args: Array[String]) : Unit = {


      val params = ParameterTool.fromArgs(args)
      val inputFile = params.get("input", "tolstoy-war-and-peace.txt")
      val outputFile = params.get("output", "count")


    
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.readTextFile(inputFile)
    // parse the data, group it, window it, and aggregate the counts 
    val windowCounts = text
          .flatMap { w => w.split("\\s") }
          .map { w => WordWithCount(w.toLowerCase(), 1)}
          .keyBy("word")
          .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)
}
