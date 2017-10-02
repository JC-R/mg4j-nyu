package edu.nyu.tandon.tool.renumber;

import com.google.common.collect.ImmutableMap;
import com.martiansoftware.jsap.JSAPException;
import edu.nyu.tandon.test.BaseTest;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Lines2BinTest extends BaseTest {

    @Test
    public void lines2bin() throws IOException, IllegalAccessException, InvocationTargetException, JSAPException, InstantiationException, ConfigurationException, URISyntaxException, NoSuchMethodException, ClassNotFoundException {

        // Given
        File input = newTemporaryFile();
        FileUtils.writeLines(
                input,
                Arrays.asList("1", "2", "0", String.valueOf(Long.MAX_VALUE)));
        File output = newTemporaryFile();

        // When
        Lines2Bin.main(String.format("%s %s",
                input.getAbsolutePath(),
                output.getAbsolutePath()
        ).split(" "));

        // Then
        assertThat(
                BinIO.loadLongs(output.getAbsolutePath()),
                equalTo(new long[] {
                        1, 2, 0, Long.MAX_VALUE
                }));
    }

}
