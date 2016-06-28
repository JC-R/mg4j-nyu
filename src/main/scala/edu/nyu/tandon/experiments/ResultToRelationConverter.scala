package edu.nyu.tandon.experiments

/**
  * Convert the following result format:
  * [ result ]
  * [ result ] ...
  * to relation-like format with two columns:
  * queryNumber | result
  *
  * Note: It reads from STDIN and writes to STDOUT
  *
  * @author michal.siedlaczek@nyu.edu
  */
object ResultToRelationConverter {

  def main(args: Array[String]) =
    for {
      (ln, i) <- io.Source.stdin.getLines.zipWithIndex
    } for {
      n <- ln.split("\\s+") map (_.trim)
    } println(i + " " + n)

}
