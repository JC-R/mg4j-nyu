package edu.nyu.tandon.shard.ranking.shrkc.node;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Intermediate extends Node {

    protected Node leftChild;
    protected Node rightChild;

    public Intermediate(Node leftChild, Node rightChild) {
        leftChild.setParent(this);
        rightChild.setParent(this);
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.parent = null;
    }

    @Override
    public void updateVotes(Map<Integer, Double> votes, int U, double B) {
        if (!visited) {
            visited = true;
            this.leftChild.updateVotes(votes, U, B);
            this.rightChild.updateVotes(votes, U, B);
//            if (parent != null) parent.updateVotes(votes, U + 1, B);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof Intermediate)) return false;
        Intermediate i = (Intermediate) o;
        boolean leftEquals = i.leftChild == null;
        if (leftChild != null) {
            leftEquals = leftChild.equals(i.leftChild);
        }
        boolean rightEquals = i.rightChild == null;
        if (rightChild != null) {
            rightEquals = rightChild.equals(i.rightChild);
        }
        return leftEquals && rightEquals;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 13)
                .append(leftChild)
                .append(rightChild)
                .toHashCode();
    }
}
