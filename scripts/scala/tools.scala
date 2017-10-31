// get a ML Vector's column names

// e,g, assume 'features' vector in 3rd column of DF
val field_names = df.schema.fields(2).metadata

// get a vector field names

df.select("features").schema.fields(0).metadata.getMetadata("ml_attr").getMetadata("attrs").getMetadataArray("numeric").foreach(r => println(r.getString("name")))

// get vector columns
df.select("features").map{case Row(v: Vector) => (v(0), v(1))}.toDF("termID","docID").withColumn("termID", $"termID".cast("Int")).withColumn("docID",$"docID".cast("Int"))
