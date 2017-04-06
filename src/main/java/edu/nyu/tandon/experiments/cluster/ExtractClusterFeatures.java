package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "Partition results into this many buckets."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        boolean shardDefined = jsapResult.userSpecified("shardId");
        int buckets = jsapResult.userSpecified("buckets") ? jsapResult.getInt("buckets") : 0;
        if (buckets > 0 && !shardDefined) {
            System.err.println("you must define shard if you define buckets");
            return;
        }

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[] { basename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        TerminatingQueryEngine engine =
                new TerminatingQueryEngine<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>(
                    simpleParser,
                    new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                    indexMap);
        engine.setWeights(index2Weight);
        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        }
        engine.score(scorer);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 500;
        String outputBasename = shardDefined ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");

        DocumentalClusteringStrategy strategy = null;
        if (shardDefined) {
            strategy = (DocumentalClusteringStrategy)
                    BinIO.loadObject(basename.substring(0, basename.lastIndexOf("-")) + ".strategy");
        }

        StructType schemaResults = new StructType()
                .add("query", IntegerType)
                .add("idx", IntegerType)
                .add("docid-local", LongType)
                .add("docid-global", LongType)
                .add("score", FloatType);
        StructType schemaQuery = new StructType()
                .add("query", IntegerType)
                .add("time", LongType)
                .add("maxlist1", LongType)
                .add("maxlist2", LongType)
                .add("minlist1", LongType)
                .add("minlist2", LongType)
                .add("sumlist", LongType);

        int shardId = -1;
        if (shardDefined) {
            schemaResults = schemaResults.add("shard", IntegerType);
            schemaQuery = schemaQuery.add("shard", IntegerType);
            shardId = jsapResult.getInt("shardId");
        }

        List<Row> resultRows = new ArrayList<>();
        List<Row> queryRows = new ArrayList<>();

        double bucketStep = 0.0;
        if (buckets > 0) {
            bucketStep = 1.0 / buckets;
            schemaResults = schemaResults.add("bucket", IntegerType);
        }
        int queryCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                Object[] queryRow = new Object[schemaQuery.size()];
                queryRow[schemaQuery.fieldIndex("query")] = queryCount;

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                Index index = indexMap.get(indexMap.firstKey());
                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms =
                        Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                                .stream()
                                .map(t -> {
                                    MutableString m = new MutableString(t);
                                    termProcessor.processTerm(m);
                                    return m.toString();
                                })
                                .collect(Collectors.toList());
                List<Long> listLengths = processedTerms.stream().map(term -> {
                    try {
                        return index.documents(term).frequency();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
                if (listLengths.isEmpty()) {
                    queryRow[schemaQuery.fieldIndex("maxlist1")] = 0L;
                    queryRow[schemaQuery.fieldIndex("maxlist2")] = 0L;
                    queryRow[schemaQuery.fieldIndex("minlist1")] = 0L;
                    queryRow[schemaQuery.fieldIndex("minlist2")] = 0L;
                }
                else {
                    queryRow[schemaQuery.fieldIndex("maxlist1")] = listLengths.get(0);
                    queryRow[schemaQuery.fieldIndex("minlist1")] = listLengths.get(listLengths.size() - 1);
                    if (listLengths.size() > 1) {
                        queryRow[schemaQuery.fieldIndex("maxlist2")] = listLengths.get(1);
                        queryRow[schemaQuery.fieldIndex("minlist2")] = listLengths.get(listLengths.size() - 2);
                    }
                    else {
                        queryRow[schemaQuery.fieldIndex("maxlist2")] = 0L;
                        queryRow[schemaQuery.fieldIndex("minlist2")] = 0L;
                    }
                }
                queryRow[schemaQuery.fieldIndex("sumlist")] = listLengths.stream().mapToLong(Long::longValue).sum();

                try {
                    engine.setDocumentLowerBound(null);
                    engine.setEarlyTerminationThreshold(null);
                    long start = System.currentTimeMillis();
                    engine.process(query, 0, k, r);
                    long elapsed = System.currentTimeMillis() - start;
                    queryRow[schemaQuery.fieldIndex("time")] = elapsed;
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                    throw e;
                }

                if (buckets == 0) {
                    int idx = 0;
                    for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                        Object[] resultRow = new Object[schemaResults.size()];
                        resultRow[schemaResults.fieldIndex("query")] = queryCount;
                        resultRow[schemaResults.fieldIndex("docid-local")] = dsi.document;
                        resultRow[schemaResults.fieldIndex("score")] = (float) dsi.score;
                        if (shardDefined) {
                            resultRow[schemaResults.fieldIndex("shard")] = shardId;
                            resultRow[schemaResults.fieldIndex("docid-global")] =
                                    strategy.globalPointer(shardId, dsi.document);
                        }
                        else {
                            resultRow[schemaResults.fieldIndex("docid-global")] = dsi.document;
                        }
                        resultRow[schemaResults.fieldIndex("idx")] = idx++;
                        resultRows.add(new GenericRow(resultRow));
                    }
                }
                else {
                    for (int bucket = 0; bucket < buckets; bucket++) {
                        try {
                            engine.setDocumentLowerBound(bucket * bucketStep);
                            engine.setEarlyTerminationThreshold((bucket + 1) * bucketStep);
                            engine.process(query, 0, k, r);
                        } catch (Exception e) {
                            LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                            throw e;
                        }
                        int idx = 0;
                        for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                            Object[] resultRow = new Object[schemaResults.size()];
                            resultRow[schemaResults.fieldIndex("query")] = queryCount;
                            resultRow[schemaResults.fieldIndex("docid-local")] = dsi.document;
                            resultRow[schemaResults.fieldIndex("score")] = (float) dsi.score;
                            if (shardDefined) {
                                resultRow[schemaResults.fieldIndex("shard")] = shardId;
                                resultRow[schemaResults.fieldIndex("docid-global")] =
                                        strategy.globalPointer(shardId, dsi.document);
                                resultRow[schemaResults.fieldIndex("bucket")] = bucket;
                            }
                            resultRow[schemaResults.fieldIndex("idx")] = idx++;
                            resultRows.add(new GenericRow(resultRow));
                        }
                    }
                }

                if (shardDefined) {
                    queryRow[schemaQuery.fieldIndex("shard")] = shardId;
                }
                queryRows.add(new GenericRow(queryRow));
                queryCount++;
            }
        }

        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        sparkSession.createDataFrame(queryRows, schemaQuery).sort("query").write().mode(Overwrite).parquet(outputBasename + ".queryfeatures");
        String ext = ".results";
        if (buckets > 0) {
            ext = ext + "-" + String.valueOf(buckets);
        }
        sparkSession.createDataFrame(resultRows, schemaResults).write().mode(Overwrite).parquet(outputBasename + ext);

    }

}
