package edu.nyu.tandon.shard.ranking.shrkc;

import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.shrkc.node.Document;
import edu.nyu.tandon.shard.ranking.shrkc.node.Intermediate;
import edu.nyu.tandon.shard.ranking.shrkc.node.Node;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RankS implements ShardSelector {

    /**
     * CSI query engine: only consists of x% of the original index.
     */

    protected CentralSampleIndex csi;

    protected int maxOutput = 50;
    public double B;
    public double C = 0.0001;

    public RankS(CentralSampleIndex csi, double B) {
        this.csi = csi;
        this.B = B;
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        Map<Integer, Double> scores = shardScores(query);
        return scores.entrySet().stream()
                .filter(e -> e.getValue() > C).map(HashMap.Entry::getKey).collect(Collectors.toList());
    }

    @Override
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        Map<Integer, Double> scores = new HashMap<>();
        List<Result> results = csi.runQuery(query);

        int U = 0;
        for (Result result : results) {
            double vote = result.score * Math.pow(B, U--);
            scores.put(result.shardId, scores.getOrDefault(result.shardId, 0.0) + vote);
        }

        return scores;
    }
}
