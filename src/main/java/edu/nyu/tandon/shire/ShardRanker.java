package edu.nyu.tandon.shire;

import edu.nyu.tandon.csi.CentralSampleIndex;
import edu.nyu.tandon.csi.Result;
import edu.nyu.tandon.shire.node.Node;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class ShardRanker {

    /**
     * CSI query engine: only consists of x% of the original index.
     */

    protected CentralSampleIndex csi;

    protected int maxOutput = 50;
    public double B;
    public double C = 0.0001;

    protected abstract Node transform(List<Result> results);

    public ShardRanker(CentralSampleIndex csi, double B) {
        this.csi = csi;
        this.B = B;
    }

    public List<Integer> traverseTree(Node topRanked) {

        Map<Integer, Double> votes = new HashMap<>();
        Node node = topRanked;
        int U = 0;
        while (node != null) {
            node.updateVotes(votes, U, B);
            node = node.getParent();
            U++;
        }

        return new ArrayList<>(votes.keySet()).stream()
                // Perform the cut-off
                .filter(id -> votes.get(id) > C)
                // Sort in order of decreasing votes
                .sorted((id1, id2) -> -votes.getOrDefault(id1, 0.).compareTo(votes.getOrDefault(id2, 0.)))
                .collect(Collectors.toList());
    }

    public List<Integer> cutoff(List<Integer> shards, Map<Integer, Double> votes) {
        List<Integer> result = new ArrayList<>();
        for (Integer shrad : shards) {

        }
        return result;
    }

    public List<Integer> chooseShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Node topRanked = transform(results);
        return traverseTree(topRanked);
    }

}
