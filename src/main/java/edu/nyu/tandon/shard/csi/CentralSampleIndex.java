package edu.nyu.tandon.shard.csi;

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.query.PrunedQueryEngine;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy.STRATEGY;
import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CentralSampleIndex {

    public static final Logger LOGGER = LoggerFactory.getLogger(CentralSampleIndex.class);

    protected DocumentalClusteringStrategy csiStrategy;
    protected DocumentalPartitioningStrategy clustersStrategy;

    protected String basename;
    protected QueryEngine csiEngine;

    protected int maxOutput = 10000;

    public CentralSampleIndex(String indexBasename, DocumentalClusteringStrategy csiStrategy,
                              DocumentalPartitioningStrategy clustersStrategy, Scorer scorer)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        this.csiStrategy = csiStrategy;
        this.clustersStrategy = clustersStrategy;
        this.basename = indexBasename;
        constructQueryEngine(scorer);
    }

    public void setMaxOutput(int m) {
        maxOutput = m;
    }

    public long numberOfDocuments(int clusterId) {
        return clustersStrategy.numberOfDocuments(clusterId);
    }

    public DocumentalClusteringStrategy getCsiStrategy() {
        return csiStrategy;
    }

    public DocumentalPartitioningStrategy getClustersStrategy() {
        return clustersStrategy;
    }

    protected void constructQueryEngine(Scorer s) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        String[] basenameWeight = new String[] { basename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        csiEngine = new PrunedQueryEngine(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);
        csiEngine.setWeights(index2Weight);

        Scorer scorer = s != null ? s.copy() : new BM25Scorer();
        SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        csiEngine.score(scorer);
        LOGGER.debug(String.format("CSI engine using scorer %s", scorer.getClass().getName()));
    }

    protected int resolveCluster(long csiId) {
        long globalId = csiStrategy.globalPointer(0, csiId);
        return clustersStrategy.localIndex(globalId);
    }

    public QueryEngine getQueryEngine() {
        return csiEngine;
    }

    protected List<Result> getResults(ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r) {
        List<Result> results = new ArrayList<>();
        for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> result : r) {
            results.add(new Result(result.document, result.score, resolveCluster(result.document)));
        }
        return results;
    }

    public List<Result> runQuery(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {

        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();
        csiEngine.process(query, 0, maxOutput, r);
        return getResults(r);

    }

    public String getBasename() {
        return basename;
    }

    public static CentralSampleIndex loadCSI(String csiBasename, String clustersBasename, Scorer scorer) throws IOException, ClassNotFoundException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException {
        DocumentalClusteringStrategy csiStrategy = (DocumentalClusteringStrategy) BinIO.loadObject(csiBasename + STRATEGY);
        DocumentalPartitioningStrategy clusterStrategy = (DocumentalPartitioningStrategy) BinIO.loadObject(clustersBasename + STRATEGY);
        return new CentralSampleIndex(csiBasename + "-0", csiStrategy, clusterStrategy, scorer);
    }

}
