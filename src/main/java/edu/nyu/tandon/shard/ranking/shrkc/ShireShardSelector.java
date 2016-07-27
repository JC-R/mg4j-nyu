package edu.nyu.tandon.shard.ranking.shrkc;

import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.shrkc.node.Node;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class ShireShardSelector implements ShardSelector {

    /**
     * CSI query engine: only consists of x% of the original index.
     */

    protected CentralSampleIndex csi;

    protected int maxOutput = 50;
    public double B;
    public double C = 0.0001;

    protected abstract Node transform(List<Result> results);

    public ShireShardSelector(CentralSampleIndex csi, double B) {
        this.csi = csi;
        this.B = B;
    }

    public ShireShardSelector withC(double c) {
        C = c;
        return this;
    }

    protected Map<Integer, Double> collectVotes(Node topRanked) {
        Map<Integer, Double> votes = new HashMap<>();
        Node node = topRanked;
        int U = 0;
        while (node != null) {
            node.updateVotes(votes, U, B);
            node = node.getParent();
            U++;
        }
        return votes;
    }

    protected List<Integer> traverseTree(Node topRanked) {

        Map<Integer, Double> votes = collectVotes(topRanked);

        return new ArrayList<>(votes.keySet()).stream()
                // Perform the cut-off
                .filter(id -> votes.get(id) > C)
                // Sort in order of decreasing votes
                .sorted((id1, id2) -> -votes.getOrDefault(id1, 0.).compareTo(votes.getOrDefault(id2, 0.)))
                .collect(Collectors.toList());
    }

    @Override
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Node topRanked = transform(results);
        return collectVotes(topRanked);
    }

    @Override
    public List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Node topRanked = transform(results);
        return traverseTree(topRanked);
    }

}
