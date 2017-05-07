sc.setLogLevel("INFO")
val workDir = "/home/juan/work/workspace/"

// document features
var docF = spark.read.
    csv(workDir + "cw09b.doc_features.csv").
    toDF("d_docID","d_docSize","d_docTerms","d_xdoc").
    withColumn("d_docID", $"d_docID".cast("Int")).
    withColumn("d_docSize",$"d_docSize".cast("Int")).
    withColumn("d_docTerms",$"d_docTerms".cast("Int")).
    withColumn("d_xdoc",$"d_xdoc".cast("Double")).
    withColumn("d_doc_s_t",$"d_docSize".cast("Double")/$"d_docTerms").
    withColumn("d_xdoc_s",$"d_xdoc"/$"d_docSize").
    withColumn("d_xdoc_t",$"d_xdoc"/$"d_docTerms")

// posting features
var postingF = spark.read.csv(workDir + "cw09b-postings.features").
  toDF(Seq("p_termID","p_docID","p_tfreq","p_tdfreq","p_bm25") : _*).
  withColumn("p_termID", $"p_termID".cast("Int")).
  withColumn("p_docID", $"p_docID".cast("Int")).
  withColumn("p_tfreq", $"p_tfreq".cast("Int")).
  withColumn("p_tdfreq", $"p_tdfreq".cast("Int")).
  withColumn("p_bm25", $"p_bm25".cast("Double"))


// labels for the ML @1K training

//val labels_1K = spark.read.csv(workDir+"cw09b.100K_OR.1K.ph.bins").
//  select("_c0", "_c1", "_c19", "_c20").
//  toDF(Seq("l1k_termID", "l1k_docID", "l1k_top10", "l1k_top1K") : _*).
//  withColumn("l1k_termID",$"l1k_termID".cast("Int")).
//  withColumn("l1k_docID",$"l1k_docID".cast("Int")).
//  withColumn("l1k_top10",$"l1k_top10".cast("Int")).
//  withColumn("l1k_top1K",$"l1k_top1K".cast("Int"))
//
//// labels for the ML @10 training
//val labels_10 = spark.read.csv(workDir+"cw09b.100K_OR.10.ph.bins").
//  select("_c0", "_c1", "_c19", "_c20").
//  toDF(Seq("l10_termID", "l10_docID", "l10_top10", "l10_top1K") : _*).
//  withColumn("l10_termID",$"l10_termID".cast("Int")).
//  withColumn("l10_docID",$"l10_docID".cast("Int")).
//  withColumn("l10_top10",$"l10_top10".cast("Int")).
//  withColumn("l10_top1K",$"l10_top1K".cast("Int"))

// posthit features
var phits = spark.read.csv(workDir + "cw09b.60M_OR.1K.ph.bins").
  toDF("ph_docID","ph_termID","ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
    "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","u1","u2","u3","u4","u5").
  .drop("u1","u2","u3","u4","u5")
    .select("ph_docID","ph_termID","ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
    "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","ph_top10","ph_top1K").
    .withColumn("ph_termID", $"ph_termID".cast("Int")).
    .withColumn("ph_docID", $"ph_docID".cast("Int")).
    .withColumn("ph_b1", $"ph_b1".cast("Int")).
    .withColumn("ph_b2", $"ph_b2".cast("Int")).
    .withColumn("ph_b3", $"ph_b3".cast("Int")).
    .withColumn("ph_b4", $"ph_b4".cast("Int")).
    .withColumn("ph_b5", $"ph_b5".cast("Int")).
    .withColumn("ph_b6", $"ph_b6".cast("Int")).
    .withColumn("ph_b7", $"ph_b7".cast("Int")).
    .withColumn("ph_b8", $"ph_b8".cast("Int")).
    .withColumn("ph_b9", $"ph_b9".cast("Int")).
    .withColumn("ph_b10", $"ph_b10".cast("Int")).
    .withColumn("ph_b20", $"ph_b20".cast("Int")).
    .withColumn("ph_b40", $"ph_b40".cast("Int")).
    .withColumn("ph_b80", $"ph_b80".cast("Int")).
    .withColumn("ph_b160", $"ph_b160".cast("Int")).
    .withColumn("ph_b320", $"ph_b320".cast("Int")).
    .withColumn("ph_b640", $"ph_b640".cast("Int")).
    .withColumn("ph_b1280", $"ph_b1280".cast("Int")).
    .withColumn("ph_top10", $"ph_top10".cast("Int")).
    .withColumn("ph_top1K", $"ph_top1K".cast("Int"))

// V1. the first version of this does not have aggregates, space delimiter
// V2. has aggregates, comma delimited

// dochit @1K features
var dhits = spark.read.option("delimiter"," ").csv(workDir + "cw09b.60M_OR.1K.dh.bins").
  withColumn("dh_docID", $"_c0".cast("Int")).
  withColumn("dh_b1", $"_c1".cast("Int")).
  withColumn("dh_b2", $"_c2".cast("Int")).
  withColumn("dh_b3", $"_c3".cast("Int")).
  withColumn("dh_b4", $"_c4".cast("Int")).
  withColumn("dh_b5", $"_c5".cast("Int")).
  withColumn("dh_b6", $"_c6".cast("Int")).
  withColumn("dh_b7", $"_c7".cast("Int")).
  withColumn("dh_b8", $"_c8".cast("Int")).
  withColumn("dh_b9", $"_c9".cast("Int")).
  withColumn("dh_b10", $"_c10".cast("Int")).
  withColumn("dh_b20", $"_c11".cast("Int")).
  withColumn("dh_b40", $"_c12".cast("Int")).
  withColumn("dh_b80", $"_c13".cast("Int")).
  withColumn("dh_b160", $"_c14".cast("Int")).
  withColumn("dh_b320", $"_c15".cast("Int")).
  withColumn("dh_b640", $"_c16".cast("Int")).
  withColumn("dh_b1280", $"_c17".cast("Int")).
  withColumn("dh_top10", $"dh_b1"+$"dh_b2"+$"dh_b3"+$"dh_b4"+$"dh_b5"+$"dh_b6"+$"dh_b7"+$"dh_b8"+$"dh_b9"+$"dh_b10").
  withColumn("dh_top1K", $"dh_top10"+$"dh_b20"+$"dh_b40"+$"dh_b80"+$"dh_b160"+$"dh_b320"+$"dh_b640"+$"dh_b1280").
  select("dh_docID","dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
  "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280","dh_top10","dh_top1K")

// combine
var features = postingF.
  join(docF, docF("d_docID") <=> postingF("p_docID")).
  join(dhits, dhits("dh_docID") <=> postingF("p_docID"),"left_outer").
  withColumn("dh_ds_top10",$"dh_top10".cast("Double")/$"d_docSize").
  withColumn("dh_ds_top1K",$"dh_top1K".cast("Double")/$"d_docSize").
  withColumn("dh_dt_top10",$"dh_top10".cast("Double")/$"d_docTerms").
  withColumn("dh_dt_top1K",$"dh_top1K".cast("Double")/$"d_docTerms").
  join(phits, phits("ph_docID") <=> postingF("p_docID") && phits("ph_termID") <=> postingF("p_termID"), "left_outer").
  withColumn("ph_tf_top10",$"ph_top10".cast("Double")/$"p_tfreq").
  withColumn("ph_tf_top1K",$"ph_top1K".cast("Double")/$"p_tfreq").
  withColumn("ph_tdf_top10",$"ph_top10".cast("Double")/$"p_tdfreq").
  withColumn("ph_tdf_top1K",$"ph_top1K".cast("Double")/$"p_tdfreq").
  join(labels_10, labels_10("l10_docID") <=> postingF("p_docID") && labels_10("l10_termID") <=> postingF("p_termID"),"left_outer").
  join(labels_1K, labels_1K("l1k_docID") <=> postingF("p_docID") && labels_1K("l1k_termID") <=> postingF("p_termID"),"left_outer").
  na.fill(0)


features.
  withColumn("d_docSize",$"d_docSize".cast("Double")).
  withColumn("d_docTerms",$"d_docTerms".cast("Double")).
  withColumn("p_tfreq", $"p_tfreq".cast("Double")).
  withColumn("p_tdfreq", $"p_tdfreq".cast("Double")).
  withColumn("ph_b1", $"ph_b1".cast("Double")).
  withColumn("ph_b2", $"ph_b2".cast("Double")).
  withColumn("ph_b3", $"ph_b3".cast("Double")).
  withColumn("ph_b4", $"ph_b4".cast("Double")).
  withColumn("ph_b5", $"ph_b5".cast("Double")).
  withColumn("ph_b6", $"ph_b6".cast("Double")).
  withColumn("ph_b7", $"ph_b7".cast("Double")).
  withColumn("ph_b8", $"ph_b8".cast("Double")).
  withColumn("ph_b9", $"ph_b9".cast("Double")).
  withColumn("ph_b10", $"ph_b10".cast("Double")).
  withColumn("ph_b20", $"ph_b20".cast("Double")).
  withColumn("ph_b40", $"ph_b40".cast("Double")).
  withColumn("ph_b80", $"ph_b80".cast("Double")).
  withColumn("ph_b160", $"ph_b160".cast("Double")).
  withColumn("ph_b320", $"ph_b320".cast("Double")).
  withColumn("ph_b640", $"ph_b640".cast("Double")).
  withColumn("ph_b1280", $"ph_b1280".cast("Double")).
  withColumn("ph_top10", $"ph_top10".cast("Double")).
  withColumn("ph_top1K", $"ph_top1K".cast("Double")).
  withColumn("dh_b1", $"dh_b1".cast("Double")).
  withColumn("dh_b2", $"dh_b2".cast("Double")).
  withColumn("dh_b3", $"dh_b3".cast("Double")).
  withColumn("dh_b4", $"dh_b4".cast("Double")).
  withColumn("dh_b5", $"dh_b5".cast("Double")).
  withColumn("dh_b6", $"dh_b6".cast("Double")).
  withColumn("dh_b7", $"dh_b7".cast("Double")).
  withColumn("dh_b8", $"dh_b8".cast("Double")).
  withColumn("dh_b9", $"dh_b9".cast("Double")).
  withColumn("dh_b10", $"dh_b10".cast("Double")).
  withColumn("dh_b20", $"dh_b20".cast("Double")).
  withColumn("dh_b40", $"dh_b40".cast("Double")).
  withColumn("dh_b80", $"dh_b80".cast("Double")).
  withColumn("dh_b160", $"dh_b160".cast("Double")).
  withColumn("dh_b320", $"dh_b320".cast("Double")).
  withColumn("dh_b640", $"dh_b640".cast("Double")).
  withColumn("dh_b1280", $"dh_b1280".cast("Double")).
  withColumn("dh_top10", $"dh_top10".cast("Double")).
  withColumn("dh_top1K", $"dh_top1K".cast("Double")).
  withColumn("l1k_top10",$"l1k_top10".cast("Double")).
  withColumn("l1k_top1K",$"l1k_top1K".cast("Double")).
  withColumn("l10_top10",$"l10_top10".cast("Double")).
  withColumn("l10_top1K",$"l10_top1K".cast("Double")).
  select("l1k_top10",
    "l1k_top1K",
    "l10_top10",
    "l10_top1K",
    "d_docSize",
    "d_docTerms",
    "d_xdoc",
    "d_doc_s_t",
    "d_xdoc_s",
    "d_xdoc_t",
    "p_tfreq",
    "p_tdfreq",
    "p_bm25",
    "ph_b1",
    "ph_b2",
    "ph_b3",
    "ph_b4",
    "ph_b5",
    "ph_b6",
    "ph_b7",
    "ph_b8",
    "ph_b9",
    "ph_b10",
    "ph_b20",
    "ph_b40",
    "ph_b80",
    "ph_b160",
    "ph_b320",
    "ph_b640",
    "ph_b1280",
    "ph_top10",
    "ph_top1K",
    "ph_tf_top10",
    "ph_tf_top1K",
    "ph_tdf_top10",
    "ph_tdf_top1K",
    "dh_b1",
    "dh_b2",
    "dh_b3",
    "dh_b4",
    "dh_b5",
    "dh_b6",
    "dh_b7",
    "dh_b8",
    "dh_b9",
    "dh_b10",
    "dh_b20",
    "dh_b40",
    "dh_b80",
    "dh_b160",
    "dh_b320",
    "dh_b640",
    "dh_b1280",
    "dh_top10",
    "dh_top1K",
    "dh_ds_top10",
    "dh_ds_top1K",
    "dh_dt_top10",
    "dh_dt_top1K"
  ).write.parquet(workDir+"cw09b.features.full.parquet")

// get a sample for ML
//val sample = df.sample(false,0.20)
//sample.write.parquet(workDir+"cw09b.features.sample.parquet")
