package edu.nyu.tandon.test;

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

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

}
