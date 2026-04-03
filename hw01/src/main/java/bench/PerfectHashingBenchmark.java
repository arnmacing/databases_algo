package bench;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import perfectHashing.PerfectHashTable;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class PerfectHashingBenchmark {

    @State(Scope.Benchmark)
    public static class BuildState {
        @Param({"200000", "400000", "600000", "800000"})
        public int n;

        @Param({"42"})
        public long seed;

        int[] keys;
        Integer[] values;
        int iteration;

        @Setup(Level.Trial)
        /**
         * Подготавливает данные для замера построения таблицы
         */
        public void setupTrial() {
            keys = new int[n];
            values = new Integer[n];
            SplittableRandom rnd = new SplittableRandom(seed);

            for (int i = 0; i < n; i++) {
                int key = (i << 1) - n;
                keys[i] = key;
                values[i] = rnd.nextInt();
            }
            iteration = 0;
        }

        @Setup(Level.Iteration)
        /**
         * Перемешивает пары ключ-значение
         */
        public void setupIteration() {
            shufflePairs(keys, values, seed + iteration++);
        }
    }

    @State(Scope.Thread)
    public static class LookupState {
        @Param({"200000", "400000", "600000", "800000"})
        public int n;

        @Param({"42"})
        public long seed;

        PerfectHashTable<Integer> table;
        int[] presentKeys;
        int[] absentKeys;
        int pos;
        int iteration;

        @Setup(Level.Trial)
        /**
         * Готовит таблицу и наборы ключей для чтения
         */
        public void setupTrial() {
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

            table = PerfectHashTable.build(presentKeys, values, seed);
            pos = 0;
            iteration = 0;
        }

        @Setup(Level.Iteration)
        /**
         * Меняет порядок ключей
         */
        public void setupIteration() {
            long iterSeed = seed + iteration++;
            shuffle(presentKeys, iterSeed);
            shuffle(absentKeys, iterSeed + 1);
            pos = 0;
        }

        /**
         * Возвращает следующий существующий ключ
         */
        int nextPresent() {
            int key = presentKeys[pos];
            pos++;
            if (pos == n) {
                pos = 0;
            }
            return key;
        }

        /**
         * Возвращает следующий несуществующий ключ
         */
        int nextAbsent() {
            int key = absentKeys[pos];
            pos++;
            if (pos == n) {
                pos = 0;
            }
            return key;
        }
    }

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class BuildAnalyticsState {
        public long secondaryTableSize;
        public long primaryCollisions;
        public long expansionPermille;

        @Setup(Level.Iteration)
        public void reset() {
            secondaryTableSize = 0L;
            primaryCollisions = 0L;
            expansionPermille = 0L;
        }
    }

    @Benchmark
    /**
     * время построения таблицы
     */
    public int buildIndex(BuildState state, BuildAnalyticsState analytics, Blackhole bh) {
        PerfectHashTable<Integer> table = PerfectHashTable.build(state.keys, state.values, state.seed);

        analytics.secondaryTableSize = table.secondaryTableSize();
        analytics.primaryCollisions = table.primaryCollisionCount();
        analytics.expansionPermille = Math.round(table.expansionFactor() * 1000.0);

        bh.consume(table.containsKey(state.keys[state.n / 2]));
        return table.size();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1)
    @Fork(1)
    public int buildIndexMetrics(
            BuildState state,
            BuildAnalyticsState analytics,
            Blackhole bh
    ) {
        PerfectHashTable<Integer> table =
                PerfectHashTable.build(state.keys, state.values, state.seed);

        analytics.secondaryTableSize = table.secondaryTableSize();
        analytics.primaryCollisions = table.primaryCollisionCount();
        analytics.expansionPermille = Math.round(table.expansionFactor() * 1000.0);

        bh.consume(table.containsKey(state.keys[state.n / 2]));
        return table.size();
    }

    @Benchmark
    /**
     * Чтение по существующему ключу
     */
    public void getHit(LookupState state, Blackhole bh) {
        bh.consume(state.table.get(state.nextPresent()));
    }

    @Benchmark
    /**
     * Чтение по отсутствующему ключу
     */
    public void getMiss(LookupState state, Blackhole bh) {
        bh.consume(state.table.get(state.nextAbsent()));
    }

    @Benchmark
    /**
     * Проверка наличия существующего ключа
     */
    public boolean containsHit(LookupState state) {
        return state.table.containsKey(state.nextPresent());
    }

    @Benchmark
    /**
     * Проверка наличия отсутствующего ключа
     */
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

    private static void shufflePairs(int[] keys, Integer[] values, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = keys.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);

            int keyTmp = keys[i];
            keys[i] = keys[j];
            keys[j] = keyTmp;

            Integer valueTmp = values[i];
            values[i] = values[j];
            values[j] = valueTmp;
        }
    }

    public static void main(String[] args) throws Exception {
        String[] forwarded = args == null ? new String[0] : args;
        String[] withInclude = Arrays.copyOf(forwarded, forwarded.length + 1);
        withInclude[forwarded.length] = PerfectHashingBenchmark.class.getName() + ".*";
        org.openjdk.jmh.Main.main(withInclude);
    }
}
