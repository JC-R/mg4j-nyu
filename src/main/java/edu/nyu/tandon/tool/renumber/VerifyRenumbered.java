package edu.nyu.tandon.tool.renumber;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class VerifyRenumbered {

    private String originalBasename;
    private String renumberedBasename;
    private Index original;
    private Index renumbered;

    private String[] originalTitles;
    private String[] renumberedTitles;


    public VerifyRenumbered(String originalBasename, String renumberedBasename, boolean verifyTitles) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.originalBasename = originalBasename;
        this.renumberedBasename = renumberedBasename;
        this.original = Index.getInstance(originalBasename, true, true, true);
        this.renumbered = Index.getInstance(renumberedBasename, true, true, true);
        if (verifyTitles) {
            loadTitles();
        }
    }

    public VerifyRenumbered(String originalBasename, String renumberedBasename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this(originalBasename, renumberedBasename, true);
    }

    private void loadTitles(String basename, String[] titles) throws IOException {
        String path = basename.replaceAll("-text", ".titles");
        try (FileInputStream stream = new FileInputStream(path)) {
            LineIterator titleIter = IOUtils.lineIterator(stream, StandardCharsets.UTF_8);
            int idx = 0;
            while (titleIter.hasNext()) {
                titles[idx++] = titleIter.next();
            }
            assert idx == original.numberOfDocuments;
        }
    }

    private void loadTitles() throws IOException {
        originalTitles = new String[(int)original.numberOfDocuments];
        renumberedTitles = new String[(int)renumbered.numberOfDocuments];
        loadTitles(originalBasename, originalTitles);
        loadTitles(renumberedBasename, renumberedTitles);
    }

    public void verify() throws IOException {
        assert original.numberOfTerms == renumbered.numberOfTerms;
        assert original.numberOfDocuments == renumbered.numberOfDocuments;
        assert original.numberOfPostings == renumbered.numberOfPostings;
        assert original.numberOfOccurrences == renumbered.numberOfOccurrences;
        String originalTermsFile = originalBasename + ".terms";
        String renumberedTermsFile = renumberedBasename + ".terms";
        try (IndexReader or = original.getReader();
             IndexReader rr = renumbered.getReader();
             FileInputStream of = new FileInputStream(originalTermsFile);
             FileInputStream rf = new FileInputStream(renumberedTermsFile)) {

            LineIterator oti = IOUtils.lineIterator(of, StandardCharsets.UTF_8);
            LineIterator rti = IOUtils.lineIterator(rf, StandardCharsets.UTF_8);
            IndexIterator oi, ri;
            while (true) {
                oi = or.nextIterator();
                ri = rr.nextIterator();
                if (oi == null && ri == null) break;
                else if (oi == null || ri == null) {
                    System.err.println("ERROR: Unequal number of terms.");
                    return;
                }

                String ot = oti.nextLine();
                String rt = rti.nextLine();
                assert ot.equals(rt);
                oi.term(ot);
                ri.term(rt);

                BM25Scorer os = new BM25Scorer();
                os.wrap(oi);
                BM25Scorer rs = new BM25Scorer();
                rs.wrap(ri);

                int od, rd;
                long termId = oi.termNumber();
                assert termId == ri.termNumber();
                int freq = (int)oi.frequency();
                assert freq == ri.frequency();
                List<Pair<String, Double>> op = new ArrayList<Pair<String, Double>>(freq);
                List<Pair<String, Double>> rp = new ArrayList<Pair<String, Double>>(freq);
                for (int idx = 0; idx < freq; idx++) {
                    od = (int)oi.nextDocument();
                    op.add(Pair.of(originalTitles[od], os.score()));
                    rd = (int)ri.nextDocument();
                    rp.add(Pair.of(renumberedTitles[rd], rs.score()));
                }
                Collections.sort(op);
                Collections.sort(rp);
                if (!op.equals(rp)) {
                    System.err.println("ERROR: The following posting lists are unequal:");
                    System.err.println(String.format("\tTerm ID: %d", termId));
                    System.err.print("Original: ");
                    System.err.println(Arrays.toString(op.toArray()));
                    System.err.print("Renumbered: ");
                    System.err.println(Arrays.toString(rp.toArray()));
                }
            }
        }
    }

    public static void main(String[] args) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(VerifyRenumbered.class.getName(), "",
                new Parameter[]{
                        new UnflaggedOption("original", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the original index."),
                        new UnflaggedOption("renumbered", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the renumbered index."),
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String originalBasename = jsapResult.getString("original");
        String renumberedBasename = jsapResult.getString("original");

        VerifyRenumbered v = new VerifyRenumbered(originalBasename, renumberedBasename);
        v.verify();
    }

}