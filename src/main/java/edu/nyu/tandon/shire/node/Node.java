package edu.nyu.tandon.shire.node;

import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class Node {

    protected Node parent;
    protected boolean visited;

    public abstract void updateVotes(Map<Integer, Double> votes, int U, double B);

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) { this.parent = parent; }

}
