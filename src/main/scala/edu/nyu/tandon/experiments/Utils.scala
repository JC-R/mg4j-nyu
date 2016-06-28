package edu.nyu.tandon.experiments

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FileUtils.copyFile

import scala.util.Try

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object Utils {

  def lineToLongs(line: String): Seq[Long] = {
    val trimmed = line.trim
    if (trimmed.length > 0) (line.split("\\s+") map (_.trim.toLong)).toSeq
    else Seq()
  }

  def longsToLine(longs: Seq[Long]): String = longs mkString " "

  def backup(file: File): File = {
    val backupPath = file.getAbsolutePath + ".back"
    if (!cp(file.getAbsolutePath, backupPath))
      throw new IllegalStateException("Could not make a backup copy of the file")
    else
      new File(backupPath)
  }

  def cp(oldName: String, newName: String): Boolean =
    Try(copyFile(new File(oldName), new File(newName))).isSuccess

}
