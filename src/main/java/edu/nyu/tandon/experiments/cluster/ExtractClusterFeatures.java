package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.thrift.QueryFeatures;
import edu.nyu.tandon.experiments.thrift.Result;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import edu.nyu.tandon.search.score.SequentialDependenceModelRanker;
import edu.nyu.tandon.utils.Utils;
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
import org.apache.commons.math3.util.Pair;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    public static void writeResults(int queryId, DocumentalClusteringStrategy strategy,
                                    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> queryResults,
                                    boolean shardDefined, int shardId, boolean fakeShardDefined, int fakeShardId,
                                    Integer bucket, ThriftParquetWriter<Result> resultWriter, boolean rerank,
                                    long[] queryTermIds, Index index) throws IOException {
        List<Pair<Long, Double>> results = new ArrayList<>();
        for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : queryResults) {
            results.add(new Pair<>(dsi.document, dsi.score));
        }
        if (rerank) {
            long[] documents = new long[results.size()];
            for (int idx = 0; idx < documents.length; idx++) {
                documents[idx] = results.get(idx).getFirst();
            }
            // TODO: Move params to command line
            SequentialDependenceModelRanker ranker = new SequentialDependenceModelRanker(index, 0.8, 0.1, 0.1, 0.4, 10);
            double[] scores = ranker.score(queryTermIds, documents);
            results.clear();
            for (int idx = 0; idx < documents.length; idx++) {
                results.add(new Pair<>(documents[idx], scores[idx]));
            }
            Collections.sort(results, (lhs, rhs) -> -Double.compare(lhs.getSecond(), rhs.getSecond()));
        }
        int rank = 0;
        for (Pair<Long, Double> docscores : results) {
            long doc = docscores.getFirst();
            double score = docscores.getSecond();
            Result result = new Result(queryId, rank++);
            result.setLdocid(doc);
            result.setGdocid(doc);
            result.setScore(score);
            if (shardDefined) {
                result.setShard(shardId);
                if (bucket != null) result.setGdocid(strategy.globalPointer(shardId, doc));
            } else if (fakeShardDefined) {
                result.setShard(fakeShardId);
            }
            if (bucket != null) result.setBucket(bucket);
            resultWriter.write(result);
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new Switch("rerank", 'r', "rerank", "Rerank using Sequential Dependence Model."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("fakeShardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "fake-shard-id", "Just used to fill shard column still operates on full index"),
                        new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "Partition results into this many buckets."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        boolean rerank = jsapResult.userSpecified("rerank");
        boolean shardDefined = jsapResult.userSpecified("shardId");
        boolean fakeShardDefined = jsapResult.userSpecified("fakeShardId");
        int buckets = jsapResult.userSpecified("buckets") ? jsapResult.getInt("buckets") : 0;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[]{basename};

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
        String ext = "";
        if (buckets > 0) {
            ext = "-" + String.valueOf(buckets);
        }

        String resultPath = outputBasename + ".results" + ext;
        String queryFeaturesPath = outputBasename + ".queryfeatures" + ext;

        if (FileUtils.fileExists(resultPath)) {
            FileUtils.fileDelete(resultPath);
        }
        if (FileUtils.fileExists(queryFeaturesPath)) {
            FileUtils.fileDelete(queryFeaturesPath);
        }

        ThriftParquetWriter<Result> resultWriter = new ThriftParquetWriter<>(new org.apache.hadoop.fs.Path(resultPath),
                Result.class, CompressionCodecName.SNAPPY);
        ThriftParquetWriter<QueryFeatures> queryFeaturesWriter = new ThriftParquetWriter<>(new org.apache.hadoop.fs.Path(queryFeaturesPath),
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
        try (BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                System.out.println(String.format("Extracting features of query %d: %s", queryCount, query));

                QueryFeatures queryFeatures = new QueryFeatures(queryCount);

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();

                Index index = indexMap.get(indexMap.firstKey());
                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms = Utils.extractTerms(query, termProcessor);
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
                } else {
                    queryFeatures.setMaxlist1(listLengths.get(0));
                    queryFeatures.setMinlist1(listLengths.get(listLengths.size() - 1));
                    if (listLengths.size() > 1) {
                        queryFeatures.setMaxlist2(listLengths.get(1));
                        queryFeatures.setMinlist2(listLengths.get(listLengths.size() - 2));
                    } else {
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

                int fakeShardId = fakeShardDefined ? jsapResult.getInt("fakeShardId") : -1;
                long[] termIds = new long[processedTerms.size()];
                for (int idx = 0; idx < termIds.length; ++idx) {
                    termIds[idx] = index.termMap.getLong(processedTerms.get(idx));
                }
                if (buckets == 0) {
                    writeResults(queryCount, strategy, r, shardDefined, shardId,
                            fakeShardDefined, fakeShardId, null, resultWriter,
                            rerank, termIds, index);
                } else {
                    for (int bucket = 0; bucket < buckets; bucket++) {
                        try {
                            engine.setDocumentLowerBound(bucket * bucketStep);
                            engine.setEarlyTerminationThreshold((bucket + 1) * bucketStep);
                            engine.process(query, 0, k, r);
                        } catch (Exception e) {
                            LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                            throw e;
                        }
                        writeResults(queryCount, strategy, r, shardDefined, shardId,
                                fakeShardDefined, fakeShardId, bucket, resultWriter,
                                rerank, termIds, index);
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
