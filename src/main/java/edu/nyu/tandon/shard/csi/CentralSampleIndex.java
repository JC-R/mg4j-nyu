package edu.nyu.tandon.shard.csi;

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy.STRATEGY;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CentralSampleIndex {

    protected DocumentalClusteringStrategy csiStrategy;
    protected DocumentalPartitioningStrategy clustersStrategy;

    protected QueryEngine csiEngine;

    protected int maxOutput = 10000;

    public CentralSampleIndex(String indexBasename, DocumentalClusteringStrategy csiStrategy,
                              DocumentalPartitioningStrategy clustersStrategy)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        this.csiStrategy = csiStrategy;
        this.clustersStrategy = clustersStrategy;
        constructQueryEngine(indexBasename);
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

    protected void constructQueryEngine(String indexBasename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        csiEngine = Utils.constructQueryEngine(indexBasename);
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

    public static CentralSampleIndex loadCSI(String csiBasename, String clustersBasename) throws IOException, ClassNotFoundException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException {
        DocumentalClusteringStrategy csiStrategy = (DocumentalClusteringStrategy) BinIO.loadObject(csiBasename + STRATEGY);
        SelectiveDocumentalIndexStrategy clusterStrategy = (SelectiveDocumentalIndexStrategy) BinIO.loadObject(clustersBasename + STRATEGY);
        return new CentralSampleIndex(csiBasename + "-0", csiStrategy, clusterStrategy);
    }

}
