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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.apache.commons.configuration.ConfigurationException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RenumberTest extends BaseTest {

    @Test
    public void renumber() throws IOException, IllegalAccessException, InvocationTargetException, JSAPException, InstantiationException, ConfigurationException, URISyntaxException, NoSuchMethodException, ClassNotFoundException, QueryParserException, QueryBuilderVisitorException {

        // Given
        File dir = temporaryFolder.newFolder();
        File mapping = getFileFromResourcePath("renumber/mapping");
        int k = 3;
        Map<Long, Long> partialMapping = ImmutableMap.of(
                111l, 779l,
                1005l, 890l
        );

        // When
        Renumber.main(String.format("-i %s -o %s -m %s",
                getFileFromResourcePath("index") + "/gov2-text",
                dir.getAbsolutePath() + "/renumbered",
                mapping.getAbsolutePath()
        ).split(" "));

        // Then
        Index base = Index.getInstance(getFileFromResourcePath("index") + "/gov2-text");
        Index renumbered = Index.getInstance(dir.getAbsolutePath() + "/renumbered");
        assertThat(renumbered.numberOfDocuments, equalTo(base.numberOfDocuments));
        assertThat(renumbered.numberOfOccurrences, equalTo(base.numberOfOccurrences));
        assertThat(renumbered.numberOfPostings, equalTo(base.numberOfPostings));
        assertThat(renumbered.numberOfTerms, equalTo(base.numberOfTerms));

        QueryEngine baseEngine = Utils.constructQueryEngine(getFileFromResourcePath("index") + "/gov2-text");
        QueryEngine renumberedEngine = Utils.constructQueryEngine(dir.getAbsolutePath() + "/renumbered");

        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> baseResults = new ObjectArrayList<>();
        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> renumberedResults = new ObjectArrayList<>();
        baseEngine.process("big dog", 0, k, baseResults);
        int n = renumberedEngine.process("big dog", 0, k, renumberedResults);
        for (int i = 0; i < n; i++) {
            assertThat(renumberedResults.get(i).document,
                    equalTo(partialMapping.get(baseResults.get(i).document)));
        }
    }

    @Test
    public void renumberSmall() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException, JSAPException {
        // given
        String basename = buildIndexA();
        File mapping = newTemporaryFileWithContent("1\n2\n0\n");
        String rBasename = temporaryFolder.getRoot().getAbsolutePath() + "/renumbered";

        // when
        Renumber.main(String.format("-i %s -o %s -m %s",
                basename,
                rBasename,
                mapping.getAbsolutePath()
        ).split(" "));

        // then
        Index base = Index.getInstance(basename);
        Index renumbered = Index.getInstance(rBasename);
        assertThat(renumbered.numberOfDocuments, equalTo(base.numberOfDocuments));
        assertThat(renumbered.numberOfOccurrences, equalTo(base.numberOfOccurrences));
        assertThat(renumbered.numberOfPostings, equalTo(base.numberOfPostings));
        assertThat(renumbered.numberOfTerms, equalTo(base.numberOfTerms));

        IndexReader reader = renumbered.getReader();

        IndexIterator it = reader.nextIterator();
        assertThat(it.frequency(), equalTo(2L));
        assertThat(it.nextDocument(), equalTo(1L));
        assertThat(it.nextDocument(), equalTo(2L));

        it = reader.nextIterator();
        assertThat(it.frequency(), equalTo(1L));
        assertThat(it.nextDocument(), equalTo(1L));

        it = reader.nextIterator();
        assertThat(it.frequency(), equalTo(2L));
        assertThat(it.nextDocument(), equalTo(0L));
        assertThat(it.nextDocument(), equalTo(2L));

        reader.close();
    }

}
