package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.dsi.fastutil.io.BinIO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractBucketRank {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractBucketRank.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(ExtractBucketRank.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'b', "buckets", "The number of buckets."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        DocumentalMergedCluster index = (DocumentalMergedCluster) Index.getInstance(basename);
        DocumentalClusteringStrategy strategy = (DocumentalClusteringStrategy)
                BinIO.loadObject(basename + ".strategy");

        StructType schema = new StructType()
                .add("shard", IntegerType)
                .add("bucket", IntegerType)
                .add("bucketrank", LongType);
        List<Row> rows = new ArrayList<>();
        int bucketCount = jsapResult.getInt("buckets");

        int shardId = 0;
        for (Index shard : ClusterAccessHelper.getLocalIndices(index)) {
            long bucketSize = (long) Math.ceil(Long.valueOf(shard.numberOfDocuments).doubleValue() / bucketCount);
            long[] bucketRanks = new long[bucketCount];
            for (long localDocId = 0; localDocId < shard.numberOfDocuments; localDocId++) {
                long globalId = strategy.globalPointer(shardId, localDocId);
                bucketRanks[(int) (localDocId / bucketSize)] += globalId;
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                rows.add(new GenericRowWithSchema(new Object[] {
                        shardId,
                        bucket,
                        bucketRanks[bucket] / bucketSize
                }, schema));
            }
            shardId += 1;
        }

        Dataset<Row> df = SparkSession.builder().master("local").getOrCreate().createDataFrame(rows, schema);
        String outputFilename = String.format("%s.bucketrank-%d", jsapResult.getString("output"), bucketCount);
        df.coalesce(1)
                .write()
                .mode(Overwrite)
                .parquet(outputFilename);
        Utils.unfolder(new File(outputFilename));

    }
}
