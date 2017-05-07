package edu.nyu.tandon.shard.ranking.shire;

import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.shire.node.Document;
import edu.nyu.tandon.shard.ranking.shire.node.Intermediate;
import edu.nyu.tandon.shard.ranking.shire.node.Node;

import java.util.Iterator;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RankS extends ShireShardSelector {

    public RankS(CentralSampleIndex csi, double B) {
        super(csi, B);
    }

    @Override
    protected Node transform(List<Result> results) {
        Iterator<Result> it = results.iterator();
        Node topRanked = null;
        if (it.hasNext()) {
            Result r = it.next();
            Node left = new Document(r.shardId, r.score);
            topRanked = left;
            while (it.hasNext()) {
                r = it.next();
                Node right = new Document(r.shardId, r.score);
                left = new Intermediate(left, right);
            }
        }
        return topRanked;
    }
}
