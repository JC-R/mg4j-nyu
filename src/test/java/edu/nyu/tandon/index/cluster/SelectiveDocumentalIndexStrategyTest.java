package edu.nyu.tandon.index.cluster;

import it.unimi.dsi.fastutil.io.BinIO;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import static java.util.Arrays.sort;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveDocumentalIndexStrategyTest {

    private static SelectiveDocumentalIndexStrategy strategy;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void createStrategy() throws IOException {

        ClassLoader classLoader = SelectiveDocumentalIndexStrategy.class.getClassLoader();
        File dir = new File(classLoader.getResource("clusters/numbers").getFile());
        File[] clusterFiles = dir.listFiles();
        String[] clusters = new String[clusterFiles.length];
        for (int i = 0; i < clusters.length; i++) {
            clusters[i] = clusterFiles[i].getAbsolutePath();
        }
        sort(clusters);

        strategy = SelectiveDocumentalIndexStrategy.createStrategy(clusters, true);
    }

    @Test
    public void serialize() throws IOException, ClassNotFoundException {

        // Given
        File file = folder.newFile();

        // When
        BinIO.storeObject(strategy, file);
        SelectiveDocumentalIndexStrategy actual = (SelectiveDocumentalIndexStrategy) BinIO.loadObject(file);

        // Then
        assertThat(actual.numberOfDocuments, equalTo(strategy.numberOfDocuments));
        assertThat(actual.localIndices, equalTo(strategy.localIndices));
        assertThat(actual.localPointers, equalTo(strategy.localPointers));
        assertThat(actual.globalPointers, equalTo(strategy.globalPointers));
    }

    @Test
    public void globalPointer() {
        assertThat(17l, equalTo(strategy.globalPointer(0, 0l)));
        assertThat(1085l, equalTo(strategy.globalPointer(10, 91l)));
        assertThat(417l, equalTo(strategy.globalPointer(5, 38l)));
    }

    @Test
    public void localPointer() {
        // Cluster 0
        assertThat(0l, equalTo(strategy.localPointer(17l)));
        // Cluster 10
        assertThat(91l, equalTo(strategy.localPointer(1085l)));
        // Cluster 05
        assertThat(38l, equalTo(strategy.localPointer(417l)));
    }

    @Test
    public void localIndex() {
        assertAllInTheSameCluster(Arrays.asList(new Long[] { 17l, 29l, 37l, 47l, 55l, 1080l, 1088l, 1089l }));
        assertAllInTheSameCluster(Arrays.asList(new Long[] { 10l, 22l, 24l, 1055l, 1064l, 1069l }));
        eachInDifferentCluster(Arrays.asList(new Long[] { 17l, 1l, 6l, 19l, 5l, 8l, 10l, 0l, 14l, 7l, 2l }));
    }

    public void eachInDifferentCluster(List<Long> globalIds) {
        Set<Integer> clusters = new HashSet<>();
        for (Long id : globalIds) {
            clusters.add(strategy.localIndex(id));
        }
        assertThat(
                globalIds.size(),
                equalTo(clusters.size())
        );
    }

    public void assertAllInTheSameCluster(List<Long> globalIds) {
        Iterator<Long> i = globalIds.iterator();
        if (i.hasNext()) {
            int cluster = strategy.localIndex(i.next());
            while (i.hasNext()) {
                assertThat(
                        strategy.localIndex(i.next()),
                        equalTo(cluster));
            }
        }
    }

}
