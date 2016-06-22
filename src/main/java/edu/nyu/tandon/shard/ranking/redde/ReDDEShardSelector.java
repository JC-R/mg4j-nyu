package edu.nyu.tandon.shard.ranking.redde;

import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static java.util.stream.Collectors.groupingBy;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ReDDEShardSelector implements ShardSelector {

    protected CentralSampleIndex csi;
    protected Int2LongOpenHashMap sampleSizes;
    protected int T = 10;

    public ReDDEShardSelector(CentralSampleIndex csi) {
        this.csi = csi;
        this.sampleSizes = computeSampleSizes();
    }

    public ReDDEShardSelector withT(int t) {
        T = t;
        return this;
    }

    protected Int2LongOpenHashMap computeSampleSizes() {
        Int2LongOpenHashMap sampleSizes = new Int2LongOpenHashMap();
        for (int i = 0; i < csi.getCsiStrategy().numberOfDocuments(0); i++) {
            long globalId = csi.getCsiStrategy().globalPointer(0, i);
            int shardId = csi.getClustersStrategy().localIndex(globalId);
            sampleSizes.put(shardId, sampleSizes.getOrDefault(shardId, 0l) + 1);
        }
        return sampleSizes;
    }

    protected Map<Integer, Long> computeShardCounts(List<Result> results) {
        return results.stream().collect(groupingBy(result -> result.shardId, counting()));
    }

    protected double shardWeight(int shardId) {
        if (!sampleSizes.containsKey(shardId)) {
            throw new IllegalArgumentException("Cannot get weight of this shard: its metadata has not been loaded.");
        }
        return (double) csi.numberOfDocuments(shardId) / (double) sampleSizes.get(shardId);
    }

    protected double computeShardScore(int shardId, long count) {
        return (double) count * shardWeight(shardId);
    }

    protected Map<Integer, Double> computeShardScores(Map<Integer, Long> shardCounts) {
        Map<Integer, Double> shardScores = new HashMap<>();
        for (Integer shardId : shardCounts.keySet()) {
            shardScores.put(shardId, computeShardScore(shardId, shardCounts.get(shardId)));
        }
        return shardScores;
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Map<Integer, Long> shardCounts = computeShardCounts(results);
        Map<Integer, Double> shardScores = computeShardScores(shardCounts);
        return shardScores.keySet().stream()
                .sorted((r, s) -> -shardScores.get(r).compareTo(shardScores.get(s)))
                .limit(T)
                .collect(Collectors.toList());
    }
}
