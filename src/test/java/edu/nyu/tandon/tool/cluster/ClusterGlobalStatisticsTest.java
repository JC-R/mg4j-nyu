package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.JSAPException;
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.GLOB_FREQ_EXTENSION;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.GLOB_STAT_EXTENSION;
import static it.unimi.di.big.mg4j.index.DiskBasedIndex.TERMS_EXTENSION;
import static it.unimi.di.big.mg4j.index.Index.getInstance;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongs;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongsBig;
import static java.lang.String.valueOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterGlobalStatisticsTest extends BaseTest {

    @Test
    public void globalFrequencies() throws IOException, IllegalAccessException, URISyntaxException, JSAPException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {

        // Given
        File tmp = temporaryFolder.newFolder();
        File[] indexFiles = getFileFromResourcePath("clusters").listFiles((dir, name) -> {
            return !name.matches(".*\\" + GLOB_FREQ_EXTENSION)
                    && !name.matches(".*\\" + GLOB_STAT_EXTENSION);
        });
        for (File f : indexFiles) {
            Files.copy(Paths.get(f.getAbsolutePath()),
                    Paths.get(tmp.getAbsolutePath() + "/" + f.getName()));
        }
        // Little tricks to overcome the absolute paths in the properties file
        Properties p = new Properties();
        p.load(new FileReader(tmp.getAbsolutePath() + "/gov2C.properties"));
        Properties q = new Properties();
        Iterator<String> it = p.getKeys();
        while (it.hasNext()) {
            String key = it.next();
            if (!key.equals("localindex") && !key.equals("strategy")) {
                q.addProperty(key, p.getProperty(key));
            }
        }
        for (String li : p.getStringArray("localindex")) {
            q.addProperty("localindex", tmp.getAbsolutePath() + "/" + new File(li).getName());
        }
        q.setProperty("strategy", tmp.getAbsolutePath() + "/" + new File(p.getString("strategy")).getName());
        q.save(tmp.getAbsolutePath() + "/gov2C.properties");

        // When
        ClusterGlobalStatistics.main(new String[] {
                "-g",
                getFileFromResourcePath("index").getAbsolutePath() + "/gov2-text",
                tmp.getAbsoluteFile() + "/gov2C"
        });

        // Then
        String[] globalFreq = tmp.list((dir, name) -> name.matches(".*\\" + GLOB_FREQ_EXTENSION));
        assertThat(globalFreq.length, equalTo(11));

        String[] globalStats = tmp.list((dir, name) -> name.matches(".*\\" + GLOB_STAT_EXTENSION));
        assertThat(globalStats.length, equalTo(11));

        Index globalIndex = getInstance(getFileFromResourcePath("index").getAbsolutePath() + "/gov2-text");
        SelectiveDocumentalIndexStrategy strategy =
                (SelectiveDocumentalIndexStrategy) BinIO.loadObject(tmp.getAbsoluteFile() + "/gov2C.strategy");
        LongBigArrayBigList[] globalFrequencies = new LongBigArrayBigList[11];
        for (int i = 0; i < globalFrequencies.length; i++) {
            globalFrequencies[i] = new LongBigArrayBigList(
                    loadLongsBig(tmp.getAbsoluteFile()+ "/gov2C-" + valueOf(i)
                            + GLOB_FREQ_EXTENSION));
            LineIterator j = FileUtils.lineIterator(
                    new File(tmp.getAbsoluteFile()+ "/gov2C-" + valueOf(i) + TERMS_EXTENSION));
            IndexReader indexReader = globalIndex.getReader();
            long k = 0;
            while (j.hasNext()) {
                assertThat(globalFrequencies[i].getLong(k++), equalTo(indexReader.documents(j.next()).frequency()));
            }
            indexReader.close();
        }

        long numberOfDocuments = globalIndex.numberOfDocuments;
        long numberOfOccurrences = globalIndex.numberOfOccurrences;
        for (int i = 0; i < globalFreq.length; i++) {
            long[] globalStatistics = loadLongs(tmp.getAbsoluteFile()+ "/gov2C-" + valueOf(i) + GLOB_STAT_EXTENSION);
            assertThat(globalStatistics[0], equalTo(numberOfDocuments));
            assertThat(globalStatistics[1], equalTo(numberOfOccurrences));
        }
    }

    private void assertGlobalFrequency(Index globalIndex, SelectiveDocumentalIndexStrategy strategy,
                                       LongBigArrayBigList[] globalFrequencies, long document) throws IOException {
        assertThat(globalIndex.documents(document).frequency(),
                equalTo(globalFrequencies[strategy.localIndex(document)].getLong(strategy.localPointer(document))));
    }

}
