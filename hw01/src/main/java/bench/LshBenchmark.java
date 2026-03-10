package bench;

import lsh.MinHashLshIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Locale;
import java.util.SplittableRandom;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LshBenchmark {

    private static final String[] DICT = {
            "mama", "myla", "ramu", "koshki", "lyubyat", "moloko",
            "samolety", "letayut", "vysoko", "programmirovanie", "algoritmy",
            "hash", "tablica", "indeks", "poisk", "tekst", "dubl"
    };

    @State(Scope.Benchmark)
    public static class BuildState {
        @Param({"2000", "10000"})
        public int docs;

        @Param({"12"})
        public int wordsPerDoc;

        @Param({"42"})
        public long seed;

        String[] texts;

        @Setup(Level.Trial)
        public void setup() {
            texts = new String[docs];
            SplittableRandom rnd = new SplittableRandom(seed);

            for (int i = 0; i < docs; i += 2) {
                String base = randomText(rnd, wordsPerDoc);
                texts[i] = base;
                if (i + 1 < docs) {
                    texts[i + 1] = noisyFormattingVariant(rnd, base);
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class AddState {
        @Param({"5000"})
        public int baseDocs;

        @Param({"12"})
        public int wordsPerDoc;

        @Param({"42"})
        public long seed;

        MinHashLshIndex index;
        SplittableRandom rnd;
        int nextDocId;

        @Setup(Level.Iteration)
        public void setup() {
            index = new MinHashLshIndex(5, 128, 32, seed);
            rnd = new SplittableRandom(seed ^ 0x9E3779B97F4A7C15L);

            int docId = 1;
            for (int i = 0; i < baseDocs; i++) {
                index.add(docId++, randomText(rnd, wordsPerDoc));
            }
            nextDocId = docId;
        }
    }

    @State(Scope.Thread)
    public static class SearchState {
        @Param({"1000", "3000"})
        public int docs;

        @Param({"12"})
        public int wordsPerDoc;

        @Param({"0.9"})
        public double threshold;

        @Param({"42"})
        public long seed;

        MinHashLshIndex index;
        String[] queries;
        int queryPos;

        @Setup(Level.Trial)
        public void setup() {
            index = new MinHashLshIndex(5, 128, 32, seed);
            queries = new String[docs];

            SplittableRandom rnd = new SplittableRandom(seed);
            int docId = 1;
            for (int i = 0; i < docs; i += 2) {
                String base = randomText(rnd, wordsPerDoc);
                String variant = noisyFormattingVariant(rnd, base);

                index.add(docId++, base);
                queries[i] = base;

                if (i + 1 < docs) {
                    index.add(docId++, variant);
                    queries[i + 1] = variant;
                }
            }

            queryPos = 0;
        }

        String nextQuery() {
            String query = queries[queryPos];
            queryPos++;
            if (queryPos == queries.length) {
                queryPos = 0;
            }
            return query;
        }
    }

    @Benchmark
    public void buildIndex(BuildState state, Blackhole bh) {
        MinHashLshIndex index = new MinHashLshIndex(5, 128, 32, state.seed);
        for (int i = 0; i < state.docs; i++) {
            index.add(i + 1, state.texts[i]);
        }
        bh.consume(index);
    }

    @Benchmark
    public void addDocument(AddState state) {
        state.index.add(state.nextDocId++, randomText(state.rnd, state.wordsPerDoc));
    }

    @Benchmark
    public int candidatesQuery(SearchState state) {
        return state.index.candidates(state.nextQuery()).size();
    }

    @Benchmark
    public int nearDuplicatesLsh(SearchState state) {
        return state.index.nearDuplicates(state.threshold).size();
    }

    @Benchmark
    public int nearDuplicatesFullScan(SearchState state) {
        return state.index.nearDuplicatesFullScan(state.threshold).size();
    }

    private static String randomText(SplittableRandom rnd, int words) {
        StringBuilder sb = new StringBuilder(words * 8);
        for (int i = 0; i < words; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(DICT[rnd.nextInt(DICT.length)]);
        }
        return sb.toString();
    }

    private static String noisyFormattingVariant(SplittableRandom rnd, String base) {
        String[] words = base.split(" ");
        String[] suffix = {"", "!", "!!", "...", ",", "??", ";", ":"};

        StringBuilder sb = new StringBuilder(base.length() + 16);
        for (int i = 0; i < words.length; i++) {
            String w = words[i];
            int mode = rnd.nextInt(3);

            if (mode == 0) {
                w = w.toUpperCase(Locale.ROOT);
            } else if (mode == 1 && !w.isEmpty()) {
                w = Character.toUpperCase(w.charAt(0)) + w.substring(1);
            }

            sb.append(w);
            sb.append(suffix[rnd.nextInt(suffix.length)]);

            if (i + 1 < words.length) {
                int spaces = 1 + rnd.nextInt(3);
                sb.append(" ".repeat(spaces));
            }
        }

        if (rnd.nextBoolean()) {
            sb.append(" !!!");
        }

        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        String[] forwarded = args == null ? new String[0] : args;
        String[] withInclude = Arrays.copyOf(forwarded, forwarded.length + 1);
        withInclude[forwarded.length] = LshBenchmark.class.getName() + ".*";
        org.openjdk.jmh.Main.main(withInclude);
    }
}
