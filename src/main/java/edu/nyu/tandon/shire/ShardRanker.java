package edu.nyu.tandon.shire;

import edu.nyu.tandon.csi.CentralSampleIndex;
import edu.nyu.tandon.csi.Result;
import edu.nyu.tandon.shire.node.Node;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.*;

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

        List<Integer> shards = new ArrayList<>(votes.keySet());
        shards.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer integer, Integer t1) {
                return -votes.getOrDefault(integer, 0.).compareTo(votes.getOrDefault(t1, 0.));
            }
        });
        return shards;
    }

    public List<Integer> chooseShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Node topRanked = transform(results);
        return traverseTree(topRanked);
    }

}
