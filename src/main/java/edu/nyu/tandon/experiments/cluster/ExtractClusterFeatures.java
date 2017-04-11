package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.thrift.QueryFeatures;
import edu.nyu.tandon.experiments.thrift.Result;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

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
        String ext = ".results";
        if (buckets > 0) {
            ext = ext + "-" + String.valueOf(buckets);
        }

        ThriftParquetWriter<Result> resultWriter = new ThriftParquetWriter<>(new org.apache.hadoop.fs.Path(outputBasename + ext),
                Result.class, CompressionCodecName.SNAPPY);
        ThriftParquetWriter<QueryFeatures> queryFeaturesWriter =
                new ThriftParquetWriter<>(new org.apache.hadoop.fs.Path(outputBasename + ".queryfeatures"),
                        QueryFeatures.class, CompressionCodecName.SNAPPY);

        int shardId = -1;
        if (shardDefined) {
            shardId = jsapResult.getInt("shardId");
        }


        double bucketStep = 0.0;
        if (buckets > 0) {
            bucketStep = 1.0 / buckets;
        }
        int queryCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                QueryFeatures queryFeatures = new QueryFeatures(queryCount);

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
                    queryFeatures.setMaxlist1(0L);
                    queryFeatures.setMaxlist2(0L);
                    queryFeatures.setMinlist1(0L);
                    queryFeatures.setMinlist2(0L);
                }
                else {
                    queryFeatures.setMaxlist1(listLengths.get(0));
                    queryFeatures.setMinlist1(listLengths.get(listLengths.size() - 1));
                    if (listLengths.size() > 1) {
                        queryFeatures.setMaxlist2(listLengths.get(1));
                        queryFeatures.setMinlist2(listLengths.get(listLengths.size() - 2));
                    }
                    else {
                        queryFeatures.setMaxlist2(0L);
                        queryFeatures.setMinlist2(0L);
                    }
                }
                queryFeatures.setSumlist(listLengths.stream().mapToLong(Long::longValue).sum());

                try {
                    engine.setDocumentLowerBound(null);
                    engine.setEarlyTerminationThreshold(null);
                    long start = System.currentTimeMillis();
                    engine.process(query, 0, k, r);
                    long elapsed = System.currentTimeMillis() - start;
                    queryFeatures.setTime(elapsed);
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                    throw e;
                }

                if (buckets == 0) {
                    int rank = 0;
                    for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                        Result result = new Result(queryCount, rank++);
                        result.setLdocid(dsi.document);
                        result.setGdocid(dsi.document);
                        result.setScore(dsi.score);
                        if (shardDefined) {
                            result.setShard(shardId);
                        }
                        resultWriter.write(result);
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
                        int rank = 0;
                        for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                            Result result = new Result(queryCount, rank++);
                            result.setLdocid(dsi.document);
                            result.setScore(dsi.score);
                            if (shardDefined) {
                                result.setShard(shardId);
                                result.setGdocid(strategy.globalPointer(shardId, dsi.document));
                            }
                            result.setBucket(bucket);
                            resultWriter.write(result);
                        }
                    }
                }

                if (shardDefined) {
                    queryFeatures.setShard(shardId);
                }
                queryFeaturesWriter.write(queryFeatures);
                queryCount++;
            }
        }

        queryFeaturesWriter.close();
        resultWriter.close();

    }

}
