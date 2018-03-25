package edu.nyu.tandon.test;

import com.google.common.collect.Lists;
import edu.nyu.tandon.document.StringDocumentSequence;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexWriter;
import it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.tool.Combine;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.PartitionDocumentally;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.MWHCFunction;
import it.unimi.dsi.sux4j.util.SignedFunctionStringMap;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

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
        new IndexBuilder(basename, new StringDocumentSequence(corpus))
                //.quasiSuccinctWriterFlags(CompressionFlags.valueOf(new String[]{"POSITIONS:NONE"}, CompressionFlags.DEFAULT_STANDARD_INDEX))
                .run();
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
        Map<Component,Coding> map = new EnumMap<Component,Coding>( Component.class );
        map.put(Component.POINTERS, null);
        map.put(Component.COUNTS, null);
        new PartitionDocumentally(full + "-text", cluster, strategy, cluster + ".strategy", 0,
                PartitionDocumentally.DEFAULT_BUFFER_SIZE, map,
                Combine.IndexType.QUASI_SUCCINCT, true, 32, BitStreamIndex.DEFAULT_HEIGHT,
                QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL)
                .run();
        MWHCFunction.main(new String[] { "-s", "32", cluster + "-0.mwhc", cluster + "-0.terms" });
        MWHCFunction.main(new String[] { "-s", "32", cluster + "-1.mwhc", cluster + "-1.terms" });
        SignedFunctionStringMap.main(new String[] { cluster + "-0.mwhc", cluster + "-0.termmap" });
        SignedFunctionStringMap.main(new String[] { cluster + "-1.mwhc", cluster + "-1.termmap" });

        // Copy full index's termmap
        Files.copy(Paths.get(full + "-text.termmap"), Paths.get(cluster + ".termmap"));

        return cluster;
    }
}
