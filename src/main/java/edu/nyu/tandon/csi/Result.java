package edu.nyu.tandon.csi;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Result {

    public long documentId;
    public double score;
    public int shardId;

    public Result(long documentId, double score, int shardId) {
        this.documentId = documentId;
        this.score = score;
        this.shardId = shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof Result)) return false;
        Result r = (Result) o;
        return documentId == r.documentId
                && score == r.score
                && shardId == r.shardId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 13)
                .append(documentId)
                .append(score)
                .append(shardId)
                .toHashCode();
    }

}
