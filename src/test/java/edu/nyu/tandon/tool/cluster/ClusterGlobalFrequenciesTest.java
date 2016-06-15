package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.JSAPException;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterGlobalFrequenciesTest extends BaseTest {

//    @Test
    public void globalFrequencies() throws IOException, IllegalAccessException, URISyntaxException, JSAPException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {

        // Given
        File tmp = temporaryFolder.newFolder();
        File[] indexFiles = getFilesFromDirectory("clusters");
        for (File f : indexFiles) {
            Files.copy(Paths.get(f.getAbsolutePath()),
                    Paths.get(tmp.getAbsolutePath() + "/" + f.getName()));
        }
        // Little tricks to overcome the absolute paths in the properties file
        Properties p = new Properties();
        p.load(new FileReader(tmp.getAbsolutePath() + "/gov2C.properties"));
//        p.setProperty("strategy", tmp.getAbsolutePath() + "/" + new File(p.getString("strategy")).getName());
//        for (String localIndex : p.getStringArray("localindex")) {
//            p.;
//        }
//        p.save(tmp.getAbsolutePath() + "/gov2C.properties");
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
        ClusterGlobalFrequencies.main(new String[] { tmp.getAbsoluteFile() + "/gov2C" });

        // Then

    }

}
