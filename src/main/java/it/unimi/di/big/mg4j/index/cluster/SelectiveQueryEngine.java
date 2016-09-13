package it.unimi.di.big.mg4j.index.cluster;

import edu.nyu.tandon.experiments.cluster.logger.EventLogger;
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.query.PrunedQueryEngine;
import edu.nyu.tandon.query.QueryEngine;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.search.score.QueryLikelihoodScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.LinearAggregator;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy.STRATEGY;
import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.*;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.PROPERTIES_EXTENSION;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongs;

/**
 *
 * Combines indices for clusters as well as central sample engine (CSI)
 * to choose which clusters use for queries.
 *
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveQueryEngine<T> extends QueryEngine<T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(SelectiveQueryEngine.class);

    protected DocumentalClusteringStrategy csiStrategy;
    protected SelectiveDocumentalIndexStrategy clusterStrategy;
    protected CentralSampleIndex csi;
    protected QueryEngine[] clusterEngines;
    protected ShardSelector shardSelector;
    protected List<EventLogger> eventLoggers;

    protected DocumentalMergedCluster index;
    protected String basename;

    public SelectiveQueryEngine(final QueryParser queryParser,
                                final QueryBuilderVisitor<DocumentIterator> builderVisitor,
                                final Object2ReferenceMap<String, Index> indexMap,
                                String basename,
                                String csiBasename)
            throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        super(queryParser, builderVisitor, indexMap);
        if (indexMap.size() != 1) {
            throw new RuntimeException("Index map must contain exactly one index");
        }
        Index i = indexMap.values().iterator().next();
        if (i instanceof DocumentalMergedCluster) {
            init((DocumentalMergedCluster) i, basename, csiBasename);
        }
        else {
            throw new RuntimeException(
                    String.format("Expected DocumentalMergedCluster, got %s", i.getClass().getName()));
        }
        loadClusterEngines(index, basename);
    }

    protected void init(DocumentalMergedCluster index, String basename, String csiBasename)
            throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {

        this.index = index;
        this.basename = basename;

        csiStrategy = (DocumentalClusteringStrategy) BinIO.loadObject(csiBasename + STRATEGY);
        clusterStrategy = (SelectiveDocumentalIndexStrategy) BinIO.loadObject(basename + STRATEGY);
        csi = new CentralSampleIndex(csiBasename + "-0", csiStrategy, clusterStrategy);
        shardSelector = new ReDDEShardSelector(csi);
        eventLoggers = new ArrayList<>();
    }

    public void setShardSelector(ShardSelector s) {
        shardSelector = s;
    }

    public CentralSampleIndex getCsi() {
        return csi;
    }

    public void addEventLogger(EventLogger eventLogger) { eventLoggers.add(eventLogger); }

    protected QueryEngine loadClusterEngine(Index index, String basename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        indexMap.put("I", index);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        index2Weight.put(index, 1.0);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        PrunedQueryEngine engine = new PrunedQueryEngine(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);

        engine.setWeights(index2Weight);

        Scorer scorer = this.scorer != null ? this.scorer.copy() : new BM25Scorer();
        setGlobalStatistics(scorer, basename);
        engine.score(scorer);
        LOGGER.debug(String.format("Cluster engine using scorer %s", scorer.getClass().getName()));

        return engine;
    }

    protected void setGlobalStatistics(Scorer scorer, String shardBasename) throws IOException {
        if (scorer instanceof BM25PrunedScorer) {
            BM25PrunedScorer prunedScorer = (BM25PrunedScorer) scorer;
            long[] globalStats = loadGlobalStats(shardBasename);
            LongArrayList globalFrequencies = loadGlobalFrequencies(shardBasename);
            prunedScorer.setGlobalMetrics(globalStats[0], globalStats[1], globalFrequencies);
        }
        else if (scorer instanceof QueryLikelihoodScorer) {
            QueryLikelihoodScorer qlScorer = (QueryLikelihoodScorer) scorer;
            long[] globalStats = loadGlobalStats(shardBasename);
            LongArrayList globalOccurrencies = loadGlobalOccurrencies(shardBasename);
            qlScorer.setGlobalMetrics(globalStats[1], globalOccurrencies);
        }
    }

    @Override
    public synchronized void score(final Scorer[] scorer, final double[] weight) {
//        LOGGER.debug(String.format("Setting scorer %s", scorer[0].getClass().getName()));
        super.score(scorer, weight);
        try {
            if (scorer.length > 0) loadClusterEngines(index, basename);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load cluster engines", e);
        }
    }

    @Override
    public synchronized void score(final Scorer scorer) {
        super.score(scorer);
        try {
            loadClusterEngines(index, basename);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load cluster engines", e);
        }
    }

    protected void loadClusterEngines(DocumentalMergedCluster index, String basename) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        clusterEngines = new QueryEngine[index.allIndices.length];
        for (int i = 0; i < index.allIndices.length; i++) {
            clusterEngines[i] = loadClusterEngine(index.localIndex[i], basename + "-" + String.valueOf(i));
        }
    }

    protected void convertLocalToGlobal(int shardId, ObjectArrayList<DocumentScoreInfo<T>> results) {
        for (DocumentScoreInfo<T> dsi : results) {
            dsi.document = clusterStrategy.globalPointer(shardId, dsi.document);
        }
    }

    @Override
    public int process(final String query, int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results) throws QueryParserException, QueryBuilderVisitorException, IOException {

        results.clear();

        LOGGER.debug(String.format("Selecting shards using %s of class %s",
                shardSelector.toString(),
                shardSelector.getClass().getName()));

        List<Integer> shards = shardSelector.selectShards(query);

        LOGGER.debug(String.format("Selected %d shards.", shards.size()));

        ObjectArrayList<DocumentScoreInfo<T>> cumulativeResults = new ObjectArrayList<>();
        for (Integer shardId : shards) {
            final ObjectArrayList<DocumentScoreInfo<T>> partialResults = new ObjectArrayList<>();
            clusterEngines[shardId].process(query, offset, length, partialResults);

            LOGGER.debug(String.format("Results in shard %d: %d", shardId, partialResults.size()));

            convertLocalToGlobal(shardId, partialResults);
            cumulativeResults.addAll(partialResults);
        }

        LOGGER.debug(String.format("Results in all shards: %d", cumulativeResults.size()));
        LOGGER.debug(String.format("Sorting and truncating to: %d", length));

        results.addAll(cumulativeResults.stream()
                .sorted((r, q) -> -Double.valueOf(r.score).compareTo(Double.valueOf(q.score)))
                .limit(length)
                .collect(Collectors.toList()));

        return results.size();
    }

}
