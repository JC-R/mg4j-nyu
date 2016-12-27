package edu.nyu.tandon.shard.ranking.taily;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardSelector implements ShardSelector {

    public static final Logger LOGGER = LoggerFactory.getLogger(TailyShardSelector.class);

    private TermProcessor termProcessor;
    private List<TailyShardEvaluator> shardEvaluators;
    private TailyShardEvaluator fullEvaluator;
    private StatisticalShardRepresentation fullRepresentation;

    private int nc = 400;
    private int v = 50;

    public TailyShardSelector(String basename, int shardCount) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        shardEvaluators = new ArrayList<>();
        DocumentalMergedCluster fullIndex = (DocumentalMergedCluster) Index.getInstance(basename, true, true, true);
        Index[] shards = ClusterAccessHelper.getLocalIndices(fullIndex);
        fullRepresentation = new StatisticalShardRepresentation(basename, fullIndex);
        StringMap<? extends CharSequence> termMap = DiskBasedIndex.loadStringMap(basename + DiskBasedIndex.TERMMAP_EXTENSION);
        if (termMap == null) {
            throw new IllegalArgumentException("the cluster has to have term map provided");
        }
        for (int shardId = 0; shardId < shardCount; shardId++) {
            String shardBasename = String.format("%s-%d", basename, shardId);
            shardEvaluators.add(new TailyShardEvaluator(shards[shardId],
                    new StatisticalShardRepresentation(shardBasename, shards[shardId])));
        }
        fullEvaluator = new TailyFullEvaluator(fullIndex, fullRepresentation, shardEvaluators, termMap);
        termProcessor = fullIndex.termProcessor;
    }

    public TailyShardSelector withNc(int nc) {
        this.nc = nc;
        return this;
    }

    public TailyShardSelector withV(int v) {
        this.v = v;
        return this;
    }

    private List<String> processedTerms(String query) {
        return Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                .stream()
                .map(t -> {
                    MutableString m = new MutableString(t);
                    termProcessor.processTerm(m);
                    return m.toString();
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        return shardScores(query).entrySet().stream()
                .filter(e -> e.getValue() > v)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    @Override
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        LOGGER.info(String.format("Processing query: %s", query));
        Map<Integer, Double> scores = new HashMap<>();
        List<String> terms = processedTerms(query);
        double pc = nc / fullEvaluator.all(terms);
        StatisticalShardRepresentation.TermStats fullStats;
        try {
            fullStats = fullRepresentation.queryStats(fullEvaluator.termIds(terms));
        } catch (IllegalAccessException | URISyntaxException | InstantiationException | ConfigurationException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        double sc = TailyShardEvaluator.icdf(fullStats.expectedValue - fullStats.minValue, fullStats.variance).apply(pc);
        for (int shardId = 0; shardId < shardEvaluators.size(); shardId++) {
            double estimate = shardEvaluators.get(shardId).estimateDocsAboveCutoff(terms, sc, fullStats.minValue);
            scores.put(shardId, estimate);
        }
        return scores;
    }
}
