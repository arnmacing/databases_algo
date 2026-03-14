package bench;

import lsh.MinHashLshIndex;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class LshBenchmark {

    private static final String[] DICT = {
            "mama", "myla", "ramu", "koshki", "lyubyat", "moloko",
            "samolety", "letayut", "vysoko", "programmirovanie", "algoritmy",
            "hash", "tablica", "indeks", "poisk", "tekst", "dubl",
            "database", "storage", "engine", "query", "planner", "optimizer",
            "transaction", "isolation", "consistency", "durability", "atomicity",
            "replica", "cluster", "sharding", "partition", "latency", "throughput",
            "benchmark", "workload", "dataset", "feature", "vector", "embedding",
            "similarity", "duplicate", "document", "search", "ranking", "relevance",
            "token", "shingle", "signature", "bucket", "collision", "entropy",
            "stream", "window", "pipeline", "ingest", "schema", "column",
            "row", "indexing", "snapshot", "checkpoint", "recovery", "journal",
            "cache", "buffer", "paging", "memory", "io", "mmap",
            "thread", "parallel", "batch", "service", "api", "client",
            "server", "request", "response", "timeout", "retry", "backoff",
            "monitoring", "metrics", "histogram", "percentile", "telemetry",
            "logging", "debug", "release", "stable", "version", "experiment"
    };

    @State(Scope.Benchmark)
    public static class BuildState {
        @Param({"20000", "60000"})
        public int docs;

        @Param({"16"})
        public int wordsPerDoc;

        @Param({"42"})
        public long seed;

        String[] texts;
        int iteration;

        @Setup(Level.Trial)
        public void setupTrial() {
            texts = new String[docs];
            SplittableRandom rnd = new SplittableRandom(seed);
            for (int i = 0; i < docs; i += 2) {
                String base = randomText(rnd, wordsPerDoc);
                texts[i] = base;
                if (i + 1 < docs) {
                    texts[i + 1] = noisyFormattingVariant(rnd, base);
                }
            }
            iteration = 0;
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            shuffle(texts, seed + iteration++);
        }
    }

    @State(Scope.Thread)
    public static class AddState {
        @Param({"20000"})
        public int baseDocs;

        @Param({"16"})
        public int wordsPerDoc;

        @Param({"42"})
        public long seed;

        MinHashLshIndex index;
        SplittableRandom rnd;
        int nextDocId;
        int iteration;

        @Setup(Level.Iteration)
        public void setupIteration() {
            long iterSeed = seed + iteration++;
            index = new MinHashLshIndex(5, 128, 32, seed);
            rnd = new SplittableRandom(iterSeed);

            int docId = 1;
            for (int i = 0; i < baseDocs; i++) {
                index.add(docId++, randomText(rnd, wordsPerDoc));
            }
            nextDocId = docId;
        }
    }

    @State(Scope.Thread)
    public static class QueryState {
        @Param({"20000", "60000"})
        public int docs;

        @Param({"16"})
        public int wordsPerDoc;

        @Param({"42"})
        public long seed;

        MinHashLshIndex index;
        String[] queries;
        int[] order;
        int pos;
        int iteration;

        @Setup(Level.Trial)
        public void setupTrial() {
            index = new MinHashLshIndex(5, 128, 32, seed);
            queries = new String[docs];
            order = new int[docs];

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

            for (int i = 0; i < docs; i++) {
                order[i] = i;
            }
            pos = 0;
            iteration = 0;
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            shuffle(order, seed + iteration++);
            pos = 0;
        }

        String nextQuery() {
            String query = queries[order[pos]];
            pos++;
            if (pos == queries.length) {
                pos = 0;
            }
            return query;
        }
    }

    @State(Scope.Thread)
    public static class LshDuplicatesState {
        @Param({"6000", "12000"})
        public int docs;

        @Param({"16"})
        public int wordsPerDoc;

        @Param({"0.9"})
        public double threshold;

        @Param({"42"})
        public long seed;

        String[] texts;
        int[] order;
        MinHashLshIndex index;
        int iteration;

        @Setup(Level.Trial)
        public void setupTrial() {
            texts = new String[docs];
            order = new int[docs];
            SplittableRandom rnd = new SplittableRandom(seed);

            for (int i = 0; i < docs; i += 2) {
                String base = randomText(rnd, wordsPerDoc);
                texts[i] = base;
                if (i + 1 < docs) {
                    texts[i + 1] = noisyFormattingVariant(rnd, base);
                }
            }
            for (int i = 0; i < docs; i++) {
                order[i] = i;
            }
            iteration = 0;
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            shuffle(order, seed + iteration++);
            index = new MinHashLshIndex(5, 128, 32, seed);
            int docId = 1;
            for (int id : order) {
                index.add(docId++, texts[id]);
            }
        }
    }

    @State(Scope.Thread)
    public static class FullScanState {
        @Param({"1500", "3000"})
        public int docs;

        @Param({"16"})
        public int wordsPerDoc;

        @Param({"0.9"})
        public double threshold;

        @Param({"42"})
        public long seed;

        String[] texts;
        int[] order;
        MinHashLshIndex index;
        int iteration;

        @Setup(Level.Trial)
        public void setupTrial() {
            texts = new String[docs];
            order = new int[docs];
            SplittableRandom rnd = new SplittableRandom(seed);

            for (int i = 0; i < docs; i += 2) {
                String base = randomText(rnd, wordsPerDoc);
                texts[i] = base;
                if (i + 1 < docs) {
                    texts[i + 1] = noisyFormattingVariant(rnd, base);
                }
            }
            for (int i = 0; i < docs; i++) {
                order[i] = i;
            }
            iteration = 0;
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            shuffle(order, seed + iteration++);
            index = new MinHashLshIndex(5, 128, 32, seed);
            int docId = 1;
            for (int id : order) {
                index.add(docId++, texts[id]);
            }
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
    public int candidatesQuery(QueryState state) {
        return state.index.candidates(state.nextQuery()).size();
    }

    @Benchmark
    public int nearDuplicatesLsh(LshDuplicatesState state) {
        return state.index.nearDuplicates(state.threshold).size();
    }

    @Benchmark
    public int nearDuplicatesFullScan(FullScanState state) {
        return state.index.nearDuplicatesFullScan(state.threshold).size();
    }

    private static void shuffle(int[] a, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = a.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
    }

    private static void shuffle(String[] a, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = a.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            String tmp = a[i];
            a[i] = a[j];
            a[j] = tmp;
        }
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
