package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.search.AndDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static it.unimi.di.big.mg4j.index.IndexIterator.END_OF_POSITIONS;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

public class SequentialDependenceModelRanker {

    public static final Logger LOGGER = LoggerFactory.getLogger(SequentialDependenceModelRanker.class);

    protected Index index;
    protected IndexReader reader;

    double lambdaTerm, lambdaOrdered, lambdaUnordered;
    double smoothingParameter;
    int windowSize;

    public SequentialDependenceModelRanker(Index index, double lambdaTerm,
                                           double lambdaOrdered, double lambdaUnordered,
                                           double smoothingParameter, int windowSize) throws IOException {
        this.index = index;
        this.reader = index.getReader();
        this.lambdaTerm = lambdaTerm;
        this.lambdaOrdered = lambdaOrdered;
        this.lambdaUnordered = lambdaUnordered;
        this.smoothingParameter = smoothingParameter;
        this.windowSize = windowSize;
    }

    public void close() throws IOException {
        reader.close();
    }

    public long tf(long termId, long documentId) throws IOException {
        IndexIterator iterator = reader.documents(termId);
        if (iterator.skipTo(documentId) == documentId) {
            return iterator.count();
        }
        return 0L;
    }

    public long ctf(long termId) throws IOException {
        return IndexAccessHelper.getOccurrency(reader.documents(termId));
    }

    public long collectionSize() {
        return index.numberOfOccurrences;
    }

    public long documentSize(long documentId) {
        return index.sizes.getInt(documentId);
    }

    public double smooth(double termComponent, double collectionComponent) {
        return (1.0 - smoothingParameter) * termComponent + smoothingParameter * collectionComponent;
    }

    public double potential(double lambda, long termFrequency, long documentSize, long collectionFrequency) {
        double termComponent = (double) termFrequency / (double) documentSize;
        double collectionComponent = (double) collectionFrequency / (double) collectionSize();
        return lambda * Math.log(smooth(termComponent, collectionComponent) + 1.0);
    }

    public double termPotential(long termId, long documentId) throws IOException {
        return potential(lambdaTerm, tf(termId, documentId), documentSize(documentId), ctf(termId));
    }

    public void scoreUnigrams(long[] query, long[] documentIds, double[] scores) throws IOException {
        for (int documentIdx = 0; documentIdx < documentIds.length; documentIdx++) {
            for (long termId : query) {
                scores[documentIdx] += termPotential(termId, documentIds[documentIdx]);
            }
        }
    }

    public void scoreBigrams(long[] query, long[] documentIds, double[] scores) throws IOException {
        List<Map<Long, Pair<Integer, Integer>>> maps = new ArrayList<>();
        for (int queryIdx = 0; queryIdx < query.length - 1; queryIdx++) {
            long termA = query[queryIdx];
            long termB = query[queryIdx + 1];
            maps.add(pairFrequencies(termA, termB, this.windowSize));
        }
        for (int documentIdx = 0; documentIdx < documentIds.length; documentIdx++) {
            for (int queryIdx = 0; queryIdx < query.length - 1; queryIdx++) {
                long documentId = documentIds[documentIdx];
                Pair<Integer, Integer> docFrequencies = maps.get(queryIdx).get(documentId);
                Pair<Integer, Integer> colFrequencies = maps.get(queryIdx).get(-1L);
                int docOrderedCount = docFrequencies != null ? docFrequencies.getFirst() : 0;
                int docUnorderedCount = docFrequencies != null ? docFrequencies.getSecond() : 0;
                int colOrderedCount = colFrequencies != null ? colFrequencies.getFirst() : 0;
                int colUnorderedCount = colFrequencies != null ? colFrequencies.getSecond() : 0;
                scores[documentIdx] += potential(lambdaOrdered,
                        docOrderedCount, documentSize(documentId), colOrderedCount);
                scores[documentIdx] += potential(lambdaUnordered,
                        docUnorderedCount, documentSize(documentId), colUnorderedCount);
            }
        }
    }

    public double[] score(long[] query, long[] documentIds) throws IOException {
        double[] scores = new double[documentIds.length];
        scoreUnigrams(query, documentIds, scores);
        scoreBigrams(query, documentIds, scores);
        return scores;
    }

    public Map<Long, Pair<Integer, Integer>> pairFrequencies(long termA, long termB, int windowSize) throws IOException {
        try (IndexReader readerA = index.getReader();
             IndexReader readerB = index.getReader()) {
            Map<Long, Pair<Integer, Integer>> frequencies = new HashMap<>();
            IndexIterator iteratorA = readerA.documents(termA);
            IndexIterator iteratorB = readerB.documents(termB);
            DocumentIterator intersection = AndDocumentIterator.getInstance(iteratorA, iteratorB);
            long doc;
            int collectionOrderedCount = 0;
            int collectionUnorderedCount = 0;
            while ((doc = intersection.nextDocument()) != END_OF_LIST) {
                int orderedCount = 0;
                int unorderedCount = 0;
                PositionIntersection pi = new PositionIntersection(iteratorA, iteratorB);
                Pair<Integer, Boolean> pos;
                PairWindow window = new PairWindow(windowSize);
                while ((pos = pi.nextPosition()) != null) {
                    window.push(pos.getFirst(), pos.getSecond());
                    orderedCount += window.orderedCount();
                    unorderedCount += window.unorderedCount();
                }
                frequencies.put(doc, new Pair<>(orderedCount, unorderedCount));
                collectionOrderedCount += orderedCount;
                collectionUnorderedCount += unorderedCount;
            }
            frequencies.put(-1L, new Pair<>(collectionOrderedCount, collectionUnorderedCount));
            return frequencies;
        }
    }

    public static class PositionIntersection {
        IndexIterator a, b;
        int aPos, bPos;

        public PositionIntersection(IndexIterator a, IndexIterator b) throws IOException {
            this.a = a;
            this.b = b;
            this.aPos = this.a.nextPosition();
            this.bPos = this.b.nextPosition();
        }

        public Pair<Integer, Boolean> nextPosition() throws IOException {
            Integer pos;
            Boolean isA;
            if (aPos <= bPos) {
                if (aPos == END_OF_POSITIONS) return null;
                pos = aPos;
                isA = true;
                aPos = a.nextPosition();
            } else {
                pos = bPos;
                isA = false;
                bPos = b.nextPosition();
            }
            return new Pair<>(pos, isA);
        }
    }

    public static class PairWindow {
        protected int size;
        protected List<Integer> positions;
        protected List<Boolean> isA;
        protected int countA;
        protected int countB;
        protected int orderedDeltaCount;
        protected int unorderedDeltaCount;

        public PairWindow(int size) {
            this.size = size;
            this.positions = new LinkedList<>();
            this.isA = new LinkedList<>();
            this.countA = 0;
            this.countB = 0;
            this.orderedDeltaCount = 0;
            this.unorderedDeltaCount = 0;
        }

        public void push(int position, boolean a) {
            positions.add(position);
            isA.add(a);
            if (a) countA++;
            else countB++;
            while (position - positions.get(0) >= size) {
                positions.remove(0);
                if (isA.get(0)) countA--;
                else countB--;
                isA.remove(0);
            }
            this.orderedDeltaCount = positions.size() > 1 &&
                    positions.get(positions.size() - 2) == positions.get(positions.size() - 1) - 1 &&
                    isA.get(isA.size() - 2) && !isA.get(isA.size() - 1) ? 1 : 0;
            this.unorderedDeltaCount = isA.get(isA.size() - 1) ? countB : countA;
        }

        public int orderedCount() {
            return orderedDeltaCount;
        }

        public int unorderedCount() {
            return unorderedDeltaCount;
        }
    }
}
