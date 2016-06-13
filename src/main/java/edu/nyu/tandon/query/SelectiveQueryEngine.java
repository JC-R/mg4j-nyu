package edu.nyu.tandon.query;

import edu.nyu.tandon.experiments.logger.EventLogger;
import edu.nyu.tandon.experiments.querylistener.QueryListener;
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;

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
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.PROPERTIES_EXTENSION;

/**
 *
 * Combines indices for clusters as well as central sample engine (CSI)
 * to choose which clusters use for queries.
 *
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveQueryEngine<T> {

    protected DocumentalClusteringStrategy csiStrategy;
    protected SelectiveDocumentalIndexStrategy clusterStrategy;
    protected CentralSampleIndex csi;
    protected QueryEngine[] clusterEngines;
    protected ShardSelector shardSelector;
    protected List<EventLogger> eventLoggers;

    public SelectiveQueryEngine(String basename, String csiBasename) throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        init(basename, csiBasename);
    }

    protected void init(String basename, String csiBasename)
            throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {

        csiStrategy = (DocumentalClusteringStrategy) BinIO.loadObject(csiBasename + STRATEGY);
        clusterStrategy = (SelectiveDocumentalIndexStrategy) BinIO.loadObject(basename + STRATEGY);
        csi = new CentralSampleIndex(csiBasename + "-0", csiStrategy, clusterStrategy);
        shardSelector = new ReDDEShardSelector(csi);
        loadClusterEngines(basename);
        eventLoggers = new ArrayList<>();
    }

    public void addEventLogger(EventLogger eventLogger) { eventLoggers.add(eventLogger); }

    protected String[] resolveClusterBasenames(String basename) {
        File propertiesFile = new File(basename + PROPERTIES_EXTENSION);
        File clusterDirectory = propertiesFile.getParentFile();
        File[] clusters = clusterDirectory.listFiles((dir, name) ->
                Pattern.compile(".*-\\d+\\" + PROPERTIES_EXTENSION).matcher(name).matches());
        return Arrays.stream(clusters)
                .map(f -> f.getAbsolutePath().replaceAll("\\" + PROPERTIES_EXTENSION, ""))
                .toArray(String[]::new);
    }

    protected QueryEngine loadClusterEngine(String indexBasename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        String[] basenameWeight = new String[] { indexBasename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        Query.loadIndicesFromSpec(basenameWeight, false, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        // TODO: Use pruned query engine with GLOBAL STATISTICS!
        return new QueryEngine(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);
    }

    protected void loadClusterEngines(String basename) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        String[] basenames = resolveClusterBasenames(basename);
        clusterEngines = new QueryEngine[basenames.length];
        for (int i = 0; i < clusterEngines.length; i++) {
            clusterEngines[i] = loadClusterEngine(basenames[i]);
        }
    }

    protected void convertLocalToGlobal(int shardId, ObjectArrayList<DocumentScoreInfo<T>> results) {
        for (DocumentScoreInfo<T> dsi : results) {
            dsi.document = clusterStrategy.globalPointer(shardId, dsi.document);
        }
    }

    public int process(final String query, int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results) throws QueryParserException, QueryBuilderVisitorException, IOException {

        for (EventLogger l : eventLoggers) l.onStart(query);

        List<Integer> shards = shardSelector.selectShards(query);
        ObjectArrayList<DocumentScoreInfo<T>> cumulativeResults = new ObjectArrayList<>();
        for (Integer shardId : shards) {
            final ObjectArrayList<DocumentScoreInfo<T>> partialResults = new ObjectArrayList<>();
            clusterEngines[shardId].process(query, offset, length, partialResults);
            convertLocalToGlobal(shardId, partialResults);
            cumulativeResults.addAll(partialResults);
        }

        results.addAll(cumulativeResults.stream()
                .sorted((r, q) -> -Double.valueOf(r.score).compareTo(Double.valueOf(q.score)))
                .limit(length)
                .collect(Collectors.toList()));

        for (EventLogger l : eventLoggers) l.onEnd(results);

        return results.size();
    }

}
