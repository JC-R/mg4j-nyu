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
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static java.util.stream.Collectors.groupingBy;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ReDDEShardSelector implements ShardSelector {

    protected CentralSampleIndex csi;
    protected long[] sampleSizes;
    protected int T = -1;

    public ReDDEShardSelector(CentralSampleIndex csi) {
        this.csi = csi;
        this.sampleSizes = computeSampleSizes();
    }

    public ReDDEShardSelector withT(int t) {
        T = t;
        return this;
    }

    protected long[] computeSampleSizes() {
        long[] sampleSizes = new long[csi.getClustersStrategy().numberOfLocalIndices()];
        for (int i = 0; i < csi.getCsiStrategy().numberOfDocuments(0); i++) {
            long globalId = csi.getCsiStrategy().globalPointer(0, i);
            int shardId = csi.getClustersStrategy().localIndex(globalId);
            sampleSizes[shardId]++;
        }
        return sampleSizes;
    }

    protected Map<Integer, Long> computeShardCounts(List<Result> results) {
        Map<Integer, Long> resultCounts = results.stream().collect(groupingBy(result -> result.shardId, counting()));
        for (int shard = 0; shard < sampleSizes.length; shard++) {
            if (!resultCounts.containsKey(shard)) resultCounts.put(shard, 0L);
        }
        return resultCounts;
    }

    protected double shardWeight(int shardId) {
        return (double) csi.numberOfDocuments(shardId) / (double) sampleSizes[shardId];
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
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Map<Integer, Long> shardCounts = computeShardCounts(results);
        return computeShardScores(shardCounts);
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        Map<Integer, Double> shardScores = shardScores(query);
        Stream<Integer> shards = shardScores.keySet().stream()
                .sorted((r, s) -> -shardScores.get(r).compareTo(shardScores.get(s)));
        if (T > 0) shards = shards.limit(T);
        return shards.collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return String.format("%s { T = %d }",
                this.getClass().getName(),
                T);
    }
}
