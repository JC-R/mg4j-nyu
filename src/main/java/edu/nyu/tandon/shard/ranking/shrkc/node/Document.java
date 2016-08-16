package edu.nyu.tandon.shard.ranking.shrkc.node;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Document extends Node {

    protected int shardId;
    protected double score;

    public Document(int shardId) {
        this(shardId, 1);
    }

    public Document(int shardId, double score) {
        this.shardId = shardId;
        this.score = score;
        this.visited = false;
    }

    @Override
    public void updateVotes(Map<Integer, Double> votes, int U, double B) {
        if (!visited) {
            visited = true;
            double vote = votes.getOrDefault(shardId, .0);
            vote += score * Math.pow(B, -U);
            votes.put(shardId, vote);
//            if (parent != null) parent.updateVotes(votes, U + 1, B);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof Document)) return false;
        Document d = (Document) o;
        return this.shardId == d.shardId
                && this.score == d.score;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 13)
                .append(shardId)
                .append(score)
                .toHashCode();
    }
}
