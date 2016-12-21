package edu.nyu.tandon.shard.ranking.taily;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.index.snowball.EnglishStemmer;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.configuration.ConfigurationException;

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

    private String basename;
    private TermProcessor termProcessor = new EnglishStemmer();
    private List<TailyShardEvaluator> shardEvaluators;
    private TailyShardEvaluator fullEvaluator;
    private StatisticalShardRepresentation fullRepresentation;
    private DocumentalMergedCluster fullIndex;

    private int nc = 400;
    private int v = 50;

    public TailyShardSelector(String basename, int shardCount) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.basename = basename;
        shardEvaluators = new ArrayList<>();
        fullIndex = (DocumentalMergedCluster) Index.getInstance(basename);
        Index[] shards = ClusterAccessHelper.getLocalIndices(fullIndex);
        fullRepresentation = new StatisticalShardRepresentation(basename);
        fullEvaluator = new TailyShardEvaluator(fullIndex, fullRepresentation);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            String shardBasename = String.format("%s-%d", basename, shardId);
            shardEvaluators.add(new TailyShardEvaluator(shards[shardId], new StatisticalShardRepresentation(shardBasename)));
        }
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

    private long[] frequencies(List<String> terms) throws IOException {
        long[] frequencies = new long[terms.size()];
        for (TailyShardEvaluator evaluator : shardEvaluators) {
            long[] shardFrequencies = evaluator.frequencies(terms);
            for (int i = 0; i < frequencies.length; i++) {
                frequencies[i] += shardFrequencies[i];
            }
        }
        return frequencies;
    }

//    private double any(long[] frequencies) throws IOException {
//        return TailyShardEvaluator.any(frequencies, fullIndex.numberOfDocuments);
//    }

    private double all(List<String> terms) throws IOException {
        return TailyShardEvaluator.any(frequencies(terms), fullIndex.numberOfDocuments);
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<String> terms = processedTerms(query);
        return null;
    }

    @Override
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        Map<Integer, Double> scores = new HashMap<>();
        List<String> terms = processedTerms(query);
        double pc = nc / fullEvaluator.all(terms);
        StatisticalShardRepresentation.Term fullStats;
        try {
            fullStats = fullRepresentation.queryScore(fullEvaluator.termIds(terms));
        } catch (IllegalAccessException | URISyntaxException | InstantiationException | ConfigurationException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        double sc = fullEvaluator.icdf(fullStats.expectedValue, fullStats.variance).apply(pc);
        for (int shardId = 0; shardId < shardEvaluators.size(); shardId++) {
            double estimate = shardEvaluators.get(shardId).estimateDocsAboveCutoff(terms, sc);
            scores.put(shardId, estimate);
        }
        return scores;
    }
}
