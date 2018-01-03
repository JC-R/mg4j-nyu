package edu.nyu.tandon.experiments;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.ExtractShardScores;
import edu.nyu.tandon.experiments.thrift.QueryFeatures;
import edu.nyu.tandon.experiments.thrift.Result;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class AccumulatorHits {

    public static final Logger LOGGER = LoggerFactory.getLogger(AccumulatorHits.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The query input file.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String termFilePath = String.format("%s.terms", basename);

        Index index = Index.getInstance(basename, true, true, true);
        TermProcessor termProcessor = index.termProcessor;

        System.out.println("query,hits");

        int queryId = 0;
        String queryFile = jsapResult.getString("input");
        try (FileInputStream queryStream = new FileInputStream(queryFile);
             IndexReader indexReader = index.getReader()) {
            LineIterator queries = IOUtils.lineIterator(queryStream, StandardCharsets.UTF_8);
            while (queries.hasNext()) {
                String query = queries.nextLine();
                List<String> terms = Utils.extractTerms(query, termProcessor);
                BitSet hits = new BitSet((int) index.numberOfDocuments);
                for (String term : terms) {
                    IndexIterator indexIterator = indexReader.documents(term);
                    long doc;
                    while ((doc = indexIterator.nextDocument()) != END_OF_LIST) {
                        hits.set((int) doc);
                    }
                }
                System.out.println(String.format("%d,%d", queryId++, hits.cardinality()));
            }
        }
    }

}
