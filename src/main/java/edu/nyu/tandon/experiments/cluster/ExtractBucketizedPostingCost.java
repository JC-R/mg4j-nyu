package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractBucketizedPostingCost {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractBucketizedPostingCost.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'b', "buckets", "The number of buckets."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[]{basename};

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);

        Index index = indexMap.get(indexMap.firstKey());
        IndexReader indexReader = index.getReader();

        int bucketCount = jsapResult.getInt("buckets");
        long bucketSize = (long) Math.ceil(Long.valueOf(index.numberOfDocuments).doubleValue() / bucketCount);
        int shardId = jsapResult.userSpecified("shardId") ? jsapResult.getInt("shardId") : 0;
        String outputBasename = jsapResult.userSpecified("shardId") ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");

        StructType schema = new StructType()
                .add("query", IntegerType)
                .add("shard", IntegerType)
                .add("bucket", IntegerType)
                .add("postingcost", LongType);

        List<Row> rows = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            int queryId = 0;
            for (String query; (query = br.readLine()) != null; ) {

                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms = Utils.extractTerms(query, termProcessor);

                long[] buckets = new long[bucketCount];
                for (String term : processedTerms) {
                    DocumentIterator it = indexReader.documents(term);
                    long docId;
                    while ((docId = it.nextDocument()) != END_OF_LIST) {
                        buckets[(int) (docId / bucketSize)]++;
                    }
                }
                for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
                    rows.add(new GenericRowWithSchema(new Object[] {
                            queryId,
                            shardId,
                            bucketId,
                            buckets[bucketId]
                    }, schema));
                }
                queryId++;
            }
        }

        Dataset<Row> df = SparkSession.builder().master("local").getOrCreate().createDataFrame(rows, schema);
        df.coalesce(1)
                .write()
                .mode(Overwrite)
                .parquet(outputBasename + String.format(".postingcost-%d", bucketCount));
        Utils.unfolder(new File(outputBasename + String.format(".postingcost-%d", bucketCount)));

    }

}
