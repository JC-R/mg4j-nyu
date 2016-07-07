package edu.nyu.tandon

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @author michal.siedlaczek@nyu.edu
  */
package object spark {

  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }

  }

}
