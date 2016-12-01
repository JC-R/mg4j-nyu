# spark-shell --master local[*] --packages com.databricks:spark-csv_2.10:1.3.0 driver-memory 40g --name prune-features --conf spark.kryoserializer.buffer.max=256m

val work_dir = "/home/juan/work/experiments/features/"

val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)
val toHour   = udf((t: String) => "%04d".format(t)).take(2)

// read in posthits csv
// add features
val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2_60M_OR.ph.bins-0")

val df_terms = df1.
  withColumn("doc",toInt(df1("C0"))).
  withColumn("term",toInt(df1("C1"))).
  withColumn("b1",toInt(df1("C2"))).
  withColumn("b2",toInt(df1("C3"))).
  withColumn("b3",toInt(df1("C4"))).
  withColumn("b4",toInt(df1("C5"))).
  withColumn("b5",toInt(df1("C6"))).
  withColumn("b6",toInt(df1("C7"))).
  withColumn("b7",toInt(df1("C8"))).
  withColumn("b8",toInt(df1("C9"))).read training queries posthits
  withColumn("b9",toInt(df1("C10"))).
  withColumn("b10",toInt(df1("C11"))).
  withColumn("b20",toInt(df1("C12"))).
  withColumn("b40",toInt(df1("C13"))).
  withColumn("b80",toInt(df1("C14"))).
  withColumn("b160",toInt(df1("C15"))).
  withColumn("b320",toInt(df1("C16"))).
  withColumn("b640",toInt(df1("C17"))).
  withColumn("b1280",toInt(df1("C18"))).
  withColumn("top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
  withColumn("top1k",$"top10"+$"b20"+$"b40"+$"b80"+$"b160"+$"b320"+$"b640"+$"b1280").
  select("doc","term","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b20","b40","b80","b160","b320","b640","b1280","top10","top1k")

// read in index features (created by IndexFeatures)
val df_index_raw = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2-postings.features")
val df_index = df_inoInt(df_index_raw("C1"))).
  withColumn("term_freq",toDouble(df_index_raw("C2"))).
  withColumn("doc_term_freq",toDouble(df_index_raw("C3"))).
  withColumn("bm25",toDouble(df_index_raw("C4")))
//  select("term","doc","term_freq","doc_term_freq","bm25")
// df_postings.write.parquet(work_dir+"df_postings.parquet")

// read in document features from DocHits (could be derived from PostingHits as well)
val df_doc_raw = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2.features.csv")
val df_doc = df_doc_raw.
  withColumn("doc",toInt(df_doc_raw("C0"))).
  withColumn("d_b1",toInt(df_doc_raw("C7"))).
  withColumn("d_b2",toInt(df_doc_raw("C8"))).
  withColumn("d_b3",toInt(df_doc_raw("C9"))).
  withColumn("d_b4",toInt(df_doc_raw("C10"))).
  withColumn("d_b5",toInt(df_doc_raw("C11"))).
  withColumn("d_b6",toInt(df_doc_raw("C12"))).
  withColumn("d_b7",toInt(df_doc_raw("C13"))).
  withColumn("d_b8",toInt(df_doc_raw("C14"))).
  withColumn("d_b9",toInt(df_doc_raw("C15"))).
  withColumn("d_b10",toInt(df_doc_raw("C16"))).
  withColumn("d_b20",toInt(df_doc_raw("C17"))).
  withColumn("d_b40",toInt(df_doc_raw("C18"))).
  withColumn("d_b80",toInt(df_doc_raw("C19"))).
  withColumn("d_b160",toInt(df_doc_raw("C20"))).
  withColumn("d_b320",toInt(df_doc_raw("C21"))).
  withColumn("d_b640",toInt(df_doc_raw("C22"))).
  withColumn("d_b1280",toInt(df_doc_raw("C23"))).
  withColumn("doclen",toDouble(df_doc_raw("C24"))).
  withColumn("doc_terms",toDouble(df_doc_raw("C25"))).
  withColumn("xdoc",toDouble(df_doc_raw("C23")))
//df_doc.write.parquet(work_dir+"df_doc.parquet")

//val work_dir = "/home/juan/save/"
//val df3 = sqlContext.read.parquet(work_dir+"df_bins.parquet")
//val df_postings = sqlContext.read.parquet(work_dir+"df_postings.parquet")
//val df_doc = sqlContext.read.parquet(work_dir+"df_doc.parquet")

val df1 = df_postings.join(df_doc, df_postings("doc")<=>df_doc("doc"))
val df2 = df1.join(df3, df1("term") <=> df3("term") && df_doc("doc") <=> df3("doc"))
df2.write.format("com.databricks.spark.csv").option("header", "false").save(work_dir+"postings_features.csv")
#df2.write.parquet(work_dir+"postings_features.parquet")

header:
  term,doc,b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b20,b40,b80,b160,b320,b640,b1280,term_freq,doc_term_freq,bm25,d_b1,d_b2,d_b3,d_b4,d_b5,d_b6,d_b7,d_b8,d_b9,d_b10,d_b20,d_b40,d_b80,d_b160,d_b320,d_b640,d_b1280,doclen,doc_terms,xdoc


val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)
val work_dir = "/home/juan/work/experiments/features/"
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"postings_features.csv")
val df1 = df.
  withColumn("term",toInt(df("C0"))).
  withColumn("doc",toInt(df("C1"))).
  withColumn("term_freq",toInt(df("C2"))).
  withColumn("doc_term_freq",toInt(df("C3"))).
  withColumn("bm25",toDouble(df("C4"))).
  withColumn("b1",toInt(df("C28"))).
  withColumn("b2",toInt(df("C29"))).
  withColumn("b3",toInt(df("C30"))).
  withColumn("b4",toInt(df("C31"))).
  withColumn("b5",toInt(df("C32"))).
  withColumn("b6",toInt(df("C33"))).
  withColumn("b7",toInt(df("C34"))).
  withColumn("b8",toInt(df("C35"))).
  withColumn("b9",toInt(df("C36"))).
  withColumn("b10",toInt(df("C37"))).
  withColumn("b20",toInt(df("C38"))).
  withColumn("b40",toInt(df("C39"))).
  withColumn("b80",toInt(df("C40"))).
  withColumn("b160",toInt(df("C41"))).
  withColumn("b320",toInt(df("C42"))).
  withColumn("b640",toInt(df("C43"))).
  withColumn("b1280",toInt(df("C44"))).
  withColumn("p_top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
  withColumn("p_top20",$"p_top10"+$"b20").
  withColumn("p_top40",$"p_top20"+$"b40").
  withColumn("p_top80",$"p_top40"+$"b80").
  withColumn("p_top160",$"p_top80"+$"b160").
  withColumn("p_top320",$"p_top160"+$"b320").
  withColumn("p_top640",$"p_top320"+$"b640").
  withColumn("p_top1280",$"p_top640"+$"b1280").
  withColumn("d_b1",toInt(df("C6"))).
  withColumn("d_b2",toInt(df("C7"))).
  withColumn("d_b3",toInt(df("C8"))).
  withColumn("d_b4",toInt(df("C9"))).
  withColumn("d_b5",toInt(df("C10"))).
  withColumn("d_b6",toInt(df("C11"))).
  withColumn("d_b7",toInt(df("C12"))).
  withColumn("d_b8",toInt(df("C13"))).
  withColumn("d_b9",toInt(df("C14"))).
  withColumn("d_b10",toInt(df("C15"))).
  withColumn("d_b20",toInt(df("C16"))).
  withColumn("d_b40",toInt(df("C17"))).
  withColumn("d_b80",toInt(df("C18"))).
  withColumn("d_b160",toInt(df("C19"))).
  withColumn("d_b320",toInt(df("C20"))).
  withColumn("d_b640",toInt(df("C21"))).
  withColumn("d_b1280",toInt(df("C22"))).
  withColumn("d_top10",$"d_b1"+$"d_b2"+$"d_b3"+$"d_b4"+$"d_b5"+$"d_b6"+$"d_b7"+$"d_b8"+$"d_b9"+$"d_b10").
  withColumn("d_top20",$"d_top10"+$"d_b20").
  withColumn("d_top40",$"d_top20"+$"d_b40").
  withColumn("d_top80",$"d_top40"+$"d_b80").
  withColumn("d_top160",$"d_top80"+$"d_b160").
  withColumn("d_top320",$"d_top160"+$"d_b320").
  withColumn("d_top640",$"d_top320"+$"d_b640").
  withColumn("d_top1280",$"d_top640"+$"d_b1280").
  withColumn("doc_len",toDouble(df("C23"))).
  withColumn("doc_terms",toDouble(df("C24"))).
  withColumn("xdoc",toDouble(df("C25"))).
  withColumn("doc_ratio",$"doc_len"/$"doc_terms").
  withColumn("xdoc_ratio1",$"xdoc"/$"doc_len").
  withColumn("xdoc_ratio2",$"xdoc"/$"doc_terms").
  withColumn("top10",toInt(df("C45"))).
  withColumn("top1k",toInt(df("C46"))).
  select("term",
    "doc",
    "term_freq",
    "doc_term_freq",
    "bm25",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "b6",
    "b7",
    "b8",
    "b9",
    "b10",
    "b20",
    "b40",
    "b80",
    "b160",
    "b320",
    "b640",
    "b1280",
    "p_top10",
    "p_top20",
    "p_top40",
    "p_top80",
    "p_top160",
    "p_top320(nullable = true0",
    "d_b1",
    "d_b2",
    "d_b3",
    "d_b4",
    "d_b5",
    "d_b6",
    "d_b7",
    "d_b20",
    "d_b40",
    "d_b80",
    "d_b160",
    "d_b320",
    "d_b640",
    "d_b1280",
    "d_top10",
    "d_top20",
    "d_top40",
    "d_top80",
    "d_top160",
    "d_top320",
    "d_top640",
    "d_top1280",
    "doc_len",
    "doc_terms",
    "xdoc",
    "doc_ratio",
    "xdoc_ratio1",
    "xdoc_ratio2",
    "top10",
    "top1k")

df.write.parquet(work_dir+"postings_features.parquet")
