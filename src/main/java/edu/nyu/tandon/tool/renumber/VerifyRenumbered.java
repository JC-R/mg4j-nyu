package edu.nyu.tandon.tool.renumber;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class VerifyRenumbered {

    private String originalBasename;
    private String renumberedBasename;
    private Index original;
    private Index renumbered;

    private List<String> originalTitles;
    private List<String> renumberedTitles;


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

    private void loadTitles(String basename, List<String> titles) throws IOException {
        String path = basename.replaceAll("-text", ".titles");
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
        renumberedTitles = new ArrayList<>();
        loadTitles(originalBasename, originalTitles);
        loadTitles(renumberedBasename, renumberedTitles);
    }

    private void printAssert(String text, boolean condition) {
        String result = condition ? "OK" : "ERROR";
        System.out.println(String.format("%s: %s", text, result));
    }

    public void verify() throws IOException {
        System.out.println(String.format("Verifying %s against %s",
                renumberedBasename, originalBasename));
        printAssert("Number of terms",
                    original.numberOfTerms == renumbered.numberOfTerms);
        printAssert("Number of documents",
                    original.numberOfDocuments == renumbered.numberOfDocuments);
        printAssert("Number of postings",
                    original.numberOfPostings == renumbered.numberOfPostings);
        printAssert("Number of occurrences",
                    original.numberOfOccurrences == renumbered.numberOfOccurrences);

        String originalTermsFile = originalBasename + ".terms";
        String renumberedTermsFile = renumberedBasename + ".terms";
        int postings = 0;
        int errors = 0;
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
                    op.add(Pair.of(originalTitles.get(od), os.score()));
                    rd = (int)ri.nextDocument();
                    rp.add(Pair.of(renumberedTitles.get(rd), rs.score()));
                }
                Collections.sort(op);
                Collections.sort(rp);
                if (!op.equals(rp)) {
                    errors++;
                    System.err.println("ERROR: The following posting lists are unequal:");
                    System.err.println(String.format("\tTerm ID: %d", termId));
                    System.err.print("Original: ");
                    System.err.println(Arrays.toString(op.toArray()));
                    System.err.print("Renumbered: ");
                    System.err.println(Arrays.toString(rp.toArray()));
                }

                if (++postings % 1000 == 0) {
                    System.out.println(String.format(
                            "Verified %d postings, found %d errors.", postings, errors));
                }
            }

            System.out.println(String.format(
                    "Finished verifying %d postings, found %d errors.", postings, errors));

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
        String renumberedBasename = jsapResult.getString("renumbered");

        VerifyRenumbered v = new VerifyRenumbered(originalBasename, renumberedBasename);
        v.verify();
    }

}
