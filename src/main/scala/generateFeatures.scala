// spark-shell --master local[*] --packages com.databricks:spark-csv_2.10:1.3.0 driver-memory 40g --name prune-features --conf spark.kryoserializer.buffer.max=256m
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/"
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

// 1. read fake query posthits 
// ---------------------------
val ph_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2_60M_OR.ph.bins-0").
withColumn("docID",toInt($"C0")).
withColumn("termID",toInt($"C1")). 
withColumn("b1",toInt($"C2")).
withColumn("b2",toInt($"C3")).
withColumn("b3",toInt($"C4")).
withColumn("b4",toInt($"C5")).
withColumn("b5",toInt($"C6")).
withColumn("b6",toInt($"C7")).
withColumn("b7",toInt($"C8")).
withColumn("b8",toInt($"C9")).
withColumn("b9",toInt($"C10")).
withColumn("b10",toInt($"C11")).
withColumn("b20",toInt($"C12")).
withColumn("b40",toInt($"C13")).
withColumn("b80",toInt($"C14")).
withColumn("b160",toInt($"C15")).
withColumn("b320",toInt($"C16")).
withColumn("b640",toInt($"C17")).
withColumn("b1280",toInt($"C18")).
withColumn("top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
withColumn("top20",$"top10"+$"b20").
withColumn("top40",$"top10"+$"b40").
withColumn("top80",$"top10"+$"b80").
withColumn("top160",$"top10"+$"b160").
withColumn("top1k",$"top160"+$"b320"+$"b640"+$"b1280").
select("termID","docID","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10",
"b20","b40","b80","b160","b320","b640","b1280",
"top10","top20","top40","top80","top160","top1k")

// 2. read gov2 index features
val index_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2-postings.features").
withColumn("termID",toInt($"C0")).
withColumn("docID",toInt($"C1")).
withColumn("term_freq",toInt($"C2")).
withColumn("doc_term_freq",toInt($"C3")).
withColumn("bm25",toDouble($"C4")).
select("termID","docID","term_freq","doc_term_freq","bm25")

// 3. read document hits bins
val dh_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2.features.csv").
withColumn("docID",toInt($"C0")).
withColumn("len",toInt($"C24")).
withColumn("terms",toInt($"C25")).
withColumn("xdoc",toDouble($"C23")).
withColumn("doc_ratio", toDouble($"len")/ toDouble($"terms")).
withColumn("xdoc_norm1", toDouble($"xdoc")/ toDouble($"len")).
withColumn("xdoc_norm2", toDouble($"xdoc")/ toDouble($"terms")).
withColumn("b1",toInt($"C7")).
withColumn("b2",toInt($"C8")).
withColumn("b3",toInt($"C9")).
withColumn("b4",toInt($"C10")).
withColumn("b5",toInt($"C11")).
withColumn("b6",toInt($"C12")).
withColumn("b7",toInt($"C13")).
withColumn("b8",toInt($"C14")).
withColumn("b9",toInt($"C15")).
withColumn("b10",toInt($"C16")).
withColumn("b20",toInt($"C17")).
withColumn("b40",toInt($"C18")).
withColumn("b80",toInt($"C19")).
withColumn("b160",toInt($"C20")).
withColumn("b320",toInt($"C21")).
withColumn("b640",toInt($"C22")).
withColumn("b1280",toInt($"C23")).
withColumn("top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
withColumn("top20",$"top10"+$"b20").
withColumn("top40",$"top10"+$"b40").
withColumn("top80",$"top10"+$"b80").
withColumn("top160",$"top10"+$"b160").
withColumn("top1k",$"top160"+$"b320"+$"b640"+$"b1280").
select("docID","len","terms","xdoc","doc_ratio","xdoc_norm1","xdoc_norm2",
"b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b20","b40","b80","b160","b320","b640","b1280",
"top10","top20","top40","top80","top160","top1k")

// 4. read training queries posthits
val train_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2.100K_OR.ph.bins.csv").
toDF("termID","docID","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b20","b40","b80","b160","b320","b640","b1280","top10","top1k").
select("termID","docID","top10","top1k")

// 5. save packed
ph_features.write.parquet(work_dir+"gov2_ph_OR_60M_bins.parquet")
dh_features.write.parquet(work_dir+"gov2_dh_features.parquet")
train_features.write.parquet(work_dir+"gov2_ph_100K_OR_bins.parquet")
index_features.write.parquet(work_dir+"gov2_index_features.parquet")

// 6. join index-features + fake-query posthit features + fake query dochits features + training queries posthits features
// -----------------------------------------------------------------------------------------------------------------------
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/"
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

val index_features = sqlContext.read.parquet(work_dir+"gov2_index_features.parquet").sample(false,0.20)
val dh = sqlContext.read.parquet(work_dir+"gov2_dh_features.parquet").
toDF("d_docID","d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2",
"d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280",
"d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k")

// select a random 20% sample 
val df1 = index_features.join(dh, $"d_docID" === $"docID", "inner")
df1.cache()

val ph = sqlContext.read.parquet(work_dir+"gov2_ph_OR_60M_bins.parquet").
toDF("p_termID","p_docID","p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10",
"p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280",
"p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k")

val df2 = df1.join(ph, $"p_termID" === $"termID" && $"p_docID" === $"docID", "leftouter")
df2.cache()

val train = sqlContext.read.parquet(work_dir+"gov2_ph_100K_OR_bins.parquet").
toDF("tr_termID","tr_docID","label_top10","label_top1k")

val df = df2.join(train, $"tr_termID" === $"termID" && $"tr_docID" === $"docID", "leftouter").
select("termID","docID","term_freq","doc_term_freq","bm25",
"d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2",
"d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280",
"d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k",
"p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10","p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280",
"p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k",
"label_top10","label_top1k")

df.na.fill(0).write.format("com.databricks.spark.csv").option("header","true").save(work_dir+"gov2-train-features.csv")



