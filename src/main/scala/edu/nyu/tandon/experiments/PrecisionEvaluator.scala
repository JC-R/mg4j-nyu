package edu.nyu.tandon.experiments

import edu.nyu.tandon.experiments.Utils._

import scala.io.Source

object PrecisionEvaluator {

  def main(args: Array[String]) = {

    val fullResultsFlie = "/home/elshize/phd/experiments/gov2/full/unordered/gov2-trec_eval-queries.txt.top10"
    val shardChoicesFile = "/home/elshize/phd/experiments/gov2/clusters/unordered/gov2-trec_eval-queries.txt.shards.t10"
    val shardResultsFiles = for {i <- 0 until 50}
      yield "/home/elshize/phd/experiments/gov2/clusters/unordered/" + i + "/gov2-trec_eval-queries.txt.top10"

    val fullStream = Source.fromFile(fullResultsFlie).getLines().toStream map lineToLongs
    val shardChoicesStream = Source.fromFile(shardChoicesFile).getLines().toStream map lineToLongs
    val shardResultsStreams: Seq[Stream[Seq[Long]]] =
      for (f <- shardResultsFiles)
        yield Source.fromFile(f).getLines().toStream map lineToLongs

    val documentsForQueries = for ((shards, i) <- shardChoicesStream.zipWithIndex)
      yield (for (shard <- shards)
        yield shardResultsStreams(shard.toInt)(i)).flatten

    val precisionStream = (fullStream zip documentsForQueries) map {
      case (topResults, clusterResults) => (topResults intersect clusterResults).length / topResults.length
    }

    for {
      p <- precisionStream
    } {
      println(p)
    }

  }
}
