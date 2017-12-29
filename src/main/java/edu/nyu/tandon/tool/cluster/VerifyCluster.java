package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class VerifyCluster {

    private String originalBasename;
    private String clusterBasename;
    private Index original;
    private DocumentalCluster cluster;
    private Index[] localIndex;

    private List<String> originalTitles;
    private List<List<String>> clusterTitles;


    public VerifyCluster(String originalBasename, String clusterBasename, boolean verifyTitles) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.originalBasename = originalBasename;
        this.clusterBasename = clusterBasename;
        this.original = Index.getInstance(originalBasename, true, true, true);
        this.cluster = (DocumentalCluster) Index.getInstance(clusterBasename, true, true, true);
        this.localIndex = ClusterAccessHelper.getLocalIndices(cluster);
        if (verifyTitles) {
            loadTitles();
        }
    }

    public VerifyCluster(String originalBasename, String clusterBasename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this(originalBasename, clusterBasename, true);
    }

    private void loadTitles(String basename, List<String> titles) throws IOException {
        String path = basename.replaceAll("-text", "") + ".titles";
        try (FileInputStream stream = new FileInputStream(path)) {
            LineIterator titleIter = IOUtils.lineIterator(stream, StandardCharsets.UTF_8);
            int idx = 0;
            while (titleIter.hasNext()) {
                titles.add(titleIter.next());
            }
            assert idx == original.numberOfDocuments;
        } catch (Exception e) {
            System.err.println(String.format("Error loading titles for %s of %d documents",
                    basename, titles.size()));
            throw e;
        }
    }

    private void loadTitles() throws IOException {
        originalTitles = new ArrayList<>();
        clusterTitles = new ArrayList<>();
        loadTitles(originalBasename, originalTitles);
        for (int local = 0; local < localIndex.length; local++) {
            List<String> localTitles = new ArrayList<>();
            loadTitles(String.format("%s-%d", clusterBasename, local), localTitles);
            clusterTitles.add(localTitles);
        }
    }

    List<Pair<String, Double>> getPostings(IndexIterator indexIterator, List<String> titles) throws IOException {
        long doc;
        BM25Scorer scorer = new BM25Scorer();
        scorer.wrap(indexIterator);
        List<Pair<String, Double>> postings = new ArrayList<>();
        while ((doc = indexIterator.nextDocument()) != END_OF_LIST) {
            postings.add(Pair.of(titles.get((int)doc), scorer.score()));
        }
        return postings;
    }

    List<Pair<String, Double>> getPostings(Index index, String term, List<String> titles) throws IOException {
        List<Pair<String, Double>> postings;
        try (IndexReader reader = index.getReader()) {
            postings = getPostings(reader.documents(term), titles);
        }
        return postings;
    }

    private void printAssert(String text, boolean condition) {
        String result = condition ? "OK" : "ERROR";
        System.out.println(String.format("%s: %s", text, result));
    }

    public void verify() throws IOException {
        System.out.println(String.format("Verifying %s against %s",
                clusterBasename, originalBasename));
        printAssert("Number of terms",
                original.numberOfTerms == cluster.numberOfTerms);
        printAssert("Number of documents",
                original.numberOfDocuments == cluster.numberOfDocuments);
        printAssert("Number of postings",
                original.numberOfPostings == cluster.numberOfPostings);
        printAssert("Number of occurrences",
                original.numberOfOccurrences == cluster.numberOfOccurrences);
        List<String> terms;
        try (FileInputStream stream = new FileInputStream(originalBasename + ".terms")) {
            terms = IOUtils.readLines(stream);
        }
        Random random = new Random(1230791238L);
        for (int i = 0; i < 100000; i++) {
            int term = random.nextInt((int)original.numberOfTerms);
            List<Pair<String, Double>> originalPostings = getPostings(original, terms.get(term), originalTitles);
            List<Pair<String, Double>> clusterPostings = new ArrayList<>();
            for (int cluster = 0; cluster < localIndex.length; ++cluster) {
                List<Pair<String, Double>> localPostings =
                        getPostings(localIndex[cluster], terms.get(term), clusterTitles.get(cluster));
                clusterPostings.addAll(localPostings);
            }
            Collections.sort(originalPostings);
            Collections.sort(clusterPostings);
            if (!originalPostings.equals(clusterPostings)) {
                System.err.println("ERROR: The following posting lists are unequal:");
                System.err.println(String.format("\tTerm ID: %d", term));
                System.err.print("Original: ");
                System.err.println(Arrays.toString(originalPostings.toArray()));
                System.err.print("Cluster: ");
                System.err.println(Arrays.toString(clusterPostings.toArray()));
            }
        }
    }

    public static void main(String[] args) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(VerifyCluster.class.getName(), "",
                new Parameter[]{
                        new UnflaggedOption("original", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the original index."),
                        new UnflaggedOption("cluster", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster index."),
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String originalBasename = jsapResult.getString("original");
        String clusterBasename = jsapResult.getString("cluster");

        VerifyCluster v = new VerifyCluster(originalBasename, clusterBasename);
        v.verify();
    }

}
