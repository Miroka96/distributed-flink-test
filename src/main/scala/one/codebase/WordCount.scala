package one.codebase

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time


object WordCount {

  def main(args: Array[String]): Unit = {


    val params = ParameterTool.fromArgs(args)
    val inputFile = params.get("input", "tolstoy-war-and-peace.txt")
    val outputFile = params.get("output", "count.csv")


    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // get input data by connecting to the socket
    val text = env.readTextFile(inputFile)
    // parse the data, group it, window it, and aggregate the counts 
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w.toLowerCase().replaceAll("[^A-Za-z]", ""), 1) }
      .filter(w => w.word != "")
      .keyBy("word")
      .timeWindow(Time.days(1)).sum(1)


    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    windowCounts.writeAsCsv(outputFile).setParallelism(1)

    env.execute("WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}
