package edu.nyu.tandon.shard.ranking.redde;

import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ModifiedReDDEShardSelector extends ReDDEShardSelector {


    public ModifiedReDDEShardSelector(CentralSampleIndex csi) {
        super(csi);
    }

    @Override
    public Map<Integer, Double> shardScores(String query) throws QueryParserException, QueryBuilderVisitorException, IOException {
        List<Result> results = csi.runQuery(query);
        Map<Integer, Double> shardScores = new HashMap<>();
        for (Result result : results) {
            shardScores.put(result.shardId,
                    result.score + shardScores.getOrDefault(result.shardId, 0.0));
        }
        return shardScores;
    }

}
