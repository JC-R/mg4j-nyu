package edu.nyu.tandon.experiments.querylistener;

import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface QueryListener<T> {

    public void onQueryStart(String query);
    public void onQueryEnd(String query, ObjectArrayList<DocumentScoreInfo<T>> results);

}
