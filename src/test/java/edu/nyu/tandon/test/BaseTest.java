package edu.nyu.tandon.test;

import edu.nyu.tandon.document.StringDocumentSequence;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.tool.Combine;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

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
    public String buildIndexA() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        String basename = temporaryFolder.getRoot().getAbsolutePath() + "/A";
        buildIndex(basename, Arrays.asList(
                "a b a a g",
                "x y z a f",
                "f g h h y"
        ));
        return basename + "-text";
    }

}
