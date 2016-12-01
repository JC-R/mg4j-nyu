package edu.nyu.tandon.shard.ranking;

import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;

import java.io.IOException;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface ShardSelector {
    List<Integer> selectShards(String query) throws QueryParserException, QueryBuilderVisitorException, IOException;
}
