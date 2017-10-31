val toInt    = udf[Int, String]( _.toInt)
sc.setLogLevel("ERROR")

// read in training data
val labels_raw = spark.read.csv("scratch/cw09b.training.AND.results.txt-0.ph.txt").
  withColumn("l_dID", toInt($"_c1")).
  withColumn("l_tID", toInt($"_c2")).
  withColumn("l_rank", toInt($"_c3")).
  select("l_dID","l_tID","l_rank")

// 0 based rank
val top10_AND = labels_raw.filter($"l_rank"<10).groupBy("l_dID","l_tID").agg(count("l_rank")).toDF("l_dID","l_tID","label")
val top1k_AND = labels_raw.filter($"l_rank"<1000).groupBy("l_dID","l_tID").agg(count("l_rank")).toDF("l_dID","l_tID","label")

val ds = spark.read.parquet("scratc/cw09b.full.parquet").select("p_termID","p_docID","features")

// field names are preseved in the features vector:
//df.schema.fields(14).metadata


val df = ds.sample().join(top10_AND,"p_termDI" == "l_tID" && "p_docID" == "l_dID", "outer").select("label","features")