package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.shard.ranking.taily.TailyShardSelector;
import edu.nyu.tandon.utils.Utils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractTailyScores {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractTailyScores.class);

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("clusters", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The number of clusters."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        int clusters = jsapResult.getInt("clusters");
        TailyShardSelector shardSelector = new TailyShardSelector(jsapResult.getString("basename"), clusters);

        List<Row> rows = ExtractShardScores.run(new File(jsapResult.getString("input")), "taily", clusters, shardSelector);

        StructType schema = new StructType()
                .add("query", IntegerType)
                .add("shard", IntegerType)
                .add("taily", DoubleType);

        SparkSession.builder().master("local").getOrCreate().createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode(Overwrite)
                .parquet(jsapResult.getString("output") + ".taily");

        Utils.unfolder(new File(jsapResult.getString("output") + ".taily"));
    }
}
