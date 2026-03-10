package bench;

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
import perfectHashing.PerfectHashTable;

import java.util.SplittableRandom;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PerfectHashingBenchmark {

    @State(Scope.Benchmark)
    public static class BuildState {
        @Param({"10000", "50000"})
        public int n;

        @Param({"42"})
        public long seed;

        int[] keys;
        Integer[] values;

        @Setup(Level.Trial)
        public void setup() {
            keys = new int[n];
            values = new Integer[n];
            SplittableRandom rnd = new SplittableRandom(seed);

            for (int i = 0; i < n; i++) {
                // ключи одной четности, чтобы для miss-запросов можно было взять противоположную
                int key = (i << 1) - n;
                keys[i] = key;
                values[i] = rnd.nextInt();
            }
        }
    }

    @State(Scope.Thread)
    public static class LookupState {
        @Param({"10000", "50000"})
        public int n;

        @Param({"42"})
        public long seed;

        PerfectHashTable<Integer> table;
        int[] presentKeys;
        int[] absentKeys;
        int pos;

        @Setup(Level.Trial)
        public void setup() {
            presentKeys = new int[n];
            Integer[] values = new Integer[n];
            absentKeys = new int[n];

            SplittableRandom rnd = new SplittableRandom(seed);
            for (int i = 0; i < n; i++) {
                int key = (i << 1) - n;
                presentKeys[i] = key;
                values[i] = rnd.nextInt();
                absentKeys[i] = key + 1;
            }

            shuffle(presentKeys, seed ^ 0x9E3779B97F4A7C15L);
            shuffle(absentKeys, seed ^ 0xBF58476D1CE4E5B9L);

            table = PerfectHashTable.build(presentKeys, values, seed);
            pos = 0;
        }

        int nextPresent() {
            int key = presentKeys[pos];
            pos++;
            if (pos == n) {
                pos = 0;
            }
            return key;
        }

        int nextAbsent() {
            int key = absentKeys[pos];
            pos++;
            if (pos == n) {
                pos = 0;
            }
            return key;
        }
    }

    @Benchmark
    public int buildIndex(BuildState state, Blackhole bh) {
        PerfectHashTable<Integer> table = PerfectHashTable.build(state.keys, state.values, state.seed);
        bh.consume(table.containsKey(state.keys[state.n / 2]));
        return table.size();
    }

    @Benchmark
    public void getHit(LookupState state, Blackhole bh) {
        bh.consume(state.table.get(state.nextPresent()));
    }

    @Benchmark
    public void getMiss(LookupState state, Blackhole bh) {
        bh.consume(state.table.get(state.nextAbsent()));
    }

    @Benchmark
    public boolean containsHit(LookupState state) {
        return state.table.containsKey(state.nextPresent());
    }

    @Benchmark
    public boolean containsMiss(LookupState state) {
        return state.table.containsKey(state.nextAbsent());
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

    public static void main(String[] args) throws Exception {
        String[] forwarded = args == null ? new String[0] : args;
        String[] withInclude = Arrays.copyOf(forwarded, forwarded.length + 1);
        withInclude[forwarded.length] = PerfectHashingBenchmark.class.getName() + ".*";
        org.openjdk.jmh.Main.main(withInclude);
    }
}
