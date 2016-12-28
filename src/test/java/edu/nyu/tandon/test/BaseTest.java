package edu.nyu.tandon.test;

import com.google.common.collect.Lists;
import edu.nyu.tandon.document.StringDocumentSequence;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexWriter;
import it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.tool.Combine;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.PartitionDocumentally;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static it.unimi.dsi.fastutil.io.BinIO.loadObject;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class BaseTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public File newTemporaryFile() throws IOException {
        return temporaryFolder.newFile();
    }

    public File newTemporaryFile(String name) throws IOException {
        return temporaryFolder.newFile(name);
    }

    public File newTemporaryFileWithContent(String content) throws IOException {
        File f = temporaryFolder.newFile();
        Writer w = new FileWriter(f).append(content);
        w.close();
        return f;
    }

    public static File getFileFromResourcePath(String path) {
        ClassLoader classLoader = BaseTest.class.getClassLoader();
        return new File(classLoader.getResource(path).getFile());
    }

    public static String[] getFilePathsFromDirectory(File dir) {
        File[] files = dir.listFiles();
        String[] filePaths = new String[files.length];
        for (int i = 0; i < filePaths.length; i++) {
            filePaths[i] = files[i].getAbsolutePath();
        }
        return filePaths;
    }

    public static String[] getFilePathsFromDirectory(String dir) {
        return getFilePathsFromDirectory(getFileFromResourcePath(dir));
    }

    public static File[] getFilesFromDirectory(File dir) {
        return dir.listFiles();
    }

    public static File[] getFilesFromDirectory(String dir) {
        return getFilesFromDirectory(getFileFromResourcePath(dir));
    }

    public static CentralSampleIndex loadCSI() throws IOException, ClassNotFoundException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException {
        File csiDir = getFileFromResourcePath("csi");
        return new CentralSampleIndex(
                csiDir.getAbsolutePath() + "/csi-0",
                (DocumentalClusteringStrategy) loadObject(getFileFromResourcePath("csi/csi.strategy")),
                (DocumentalPartitioningStrategy) loadObject(getFileFromResourcePath("clusters/gov2C.strategy")),
                new BM25PrunedScorer());
    }

    public void buildIndex(String basename, List<String> corpus) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        new IndexBuilder(basename, new StringDocumentSequence(corpus)).run();
    }

    /**
     * Inverted Index:
     * a | 0[3] 1[1]
     * b | 0[1]
     * f | 1[1] 2[1]
     * g | 0[1] 2[1]
     * h | 2[2]
     * x | 1[1]
     * y | 1[1] 2[1]
     * z | 1[1]
     *
     * Corpus length: 15
     */
    public static final List<String> corpusA = Arrays.asList(
            "a b a a g",
            "x y z a f",
            "f g h h y"
    );

    /**
     * Inverted Index:
     * g | 0[2]
     * h | 0[1] 1[1]
     * r | 1[2]
     * t | 0[1] 1[1]
     * u | 0[1]
     * w | 1[1]
     *
     * Corpus length: 10
     */
    public static final List<String> corpusB = Arrays.asList(
            "g h u t g",
            "r t h r w"
    );

    public String buildIndexA() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        String basename = temporaryFolder.getRoot().getAbsolutePath() + "/A";
        buildIndex(basename, corpusA);
        return basename + "-text";
    }
    public String buildIndex1() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        String basename = temporaryFolder.getRoot().getAbsolutePath() + "/B";
        buildIndex(basename, corpusB);
        return basename + "-text";
    }

    public String buildCluster() throws Exception {

        // Build Full
        String full = temporaryFolder.getRoot().getAbsolutePath() + "/F";
        List<String> corpus = Lists.newArrayList();
        corpus.addAll(corpusA);
        corpus.addAll(corpusB);
        buildIndex(full, corpus);

        // Cluster
        String cluster = temporaryFolder.getRoot().getAbsolutePath() + "/cluster";
        ContiguousDocumentalStrategy strategy = new ContiguousDocumentalStrategy(0, 3, 5);
        BinIO.storeObject(strategy, cluster + ".strategy");
        new PartitionDocumentally(full + "-text", cluster, strategy, cluster + ".strategy", 0,
                PartitionDocumentally.DEFAULT_BUFFER_SIZE, CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX,
                Combine.IndexType.QUASI_SUCCINCT, true, 32, BitStreamIndex.DEFAULT_HEIGHT,
                QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL)
                .run();

        return cluster;

//        String a = buildIndexA();
//        String b = buildIndex1();
//        String cluster = temporaryFolder.getRoot().getAbsolutePath() + "/index";
//
////        List<Long> c0 = Arrays.asList(0L, 1L, 2L);
////        List<Long> c1 = Arrays.asList(3L, 4L);
////        SelectiveDocumentalIndexStrategy strategy = SelectiveDocumentalIndexStrategy.constructStrategy(
////                new Iterator[] { c0.iterator(), c1.iterator() }, 5);
//        ContiguousDocumentalStrategy strategy = new ContiguousDocumentalStrategy(0, 3);
//        BinIO.storeObject(strategy, cluster + ".strategy");
//
//        Properties properties = new Properties();
//        properties.setProperty("field", "text");
//        properties.setProperty("strategy", cluster + ".strategy");
//        properties.setProperty("localindex", a);
//        properties.setProperty("localindex", b);
//        properties.setProperty("bloom", "false");
//        properties.setProperty("indexclass", "it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster");
//        properties.setProperty("documents", "5");
//        properties.setProperty("flat", "false");
//        properties.setProperty("terms", "12");
//        properties.setProperty("postings", "20");
//        properties.setProperty("occurrences", "25");
//        properties.setProperty("maxcount", "2");
//        properties.setProperty("termprocessor", "it.unimi.di.big.mg4j.index.DowncaseTermProcessor");
//        properties.store(new FileWriter(cluster + ".properties"), null);
//
//        return cluster;
    }
}
