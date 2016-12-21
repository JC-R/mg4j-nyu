package edu.nyu.tandon.shard.ranking.taily;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.dsi.big.util.StringMap;
import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyFullEvaluator extends TailyShardEvaluator {

//    protected  Index[] shards;
    private List<TailyShardEvaluator> shardEvaluators;

    public TailyFullEvaluator(DocumentalMergedCluster index, StatisticalShardRepresentation statisticalRepresentation,
                              List<TailyShardEvaluator> shardEvaluators, StringMap<? extends CharSequence> termMap) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        super(index, statisticalRepresentation, termMap);
//        shards = ClusterAccessHelper.getLocalIndices(index);
        this.shardEvaluators = shardEvaluators;
    }

    @Override
    public long[] frequencies(List<String> terms) throws IOException {
        long[] f = new long[terms.size()];
        for (TailyShardEvaluator shardEvaluator : shardEvaluators) {
            long[] shardFreq = shardEvaluator.frequencies(terms);
            for (int i = 0; i < terms.size(); i++) f[i] += shardFreq[i];
        }
        return f;
    }

    @Override
    protected long[] frequencies(long[] terms) throws IOException {
        throw new UnsupportedOperationException();
    }
}
