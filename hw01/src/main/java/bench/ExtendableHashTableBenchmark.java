package bench;

import extendableHashing.ExtendableHashTable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 12, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class ExtendableHashTableBenchmark {
    private static final int PREFETCH_STRIDE = 64;

    @State(Scope.Thread)
    public static class ReadState {
        @Param({"8", "16"})
        public int bucketCapacity;

        @Param({"200000", "350000"})
        public int n;

        @Param({"42"})
        public long seed;

        ExtendableHashTable ht;
        int[] presentKeys;
        int[] absentKeys;
        int p;
        int iteration;

        @Setup(Level.Trial)
        /**
         * Создаёт таблицу, заполняет её и подготавливает ключи (getHit/getMiss)
         */
        public void setupTrial() {
            ht = new ExtendableHashTable(bucketCapacity);
            presentKeys = new int[n];
            absentKeys = new int[n];

            for (int i = 0; i < n; i++) {
                presentKeys[i] = i;
                absentKeys[i] = i + n;
            }

            for (int key : presentKeys) {
                ht.put(key, key);
            }

            prefault(ht, presentKeys);
            prefault(ht, absentKeys);
            p = 0;
            iteration = 0;
        }

        @Setup(Level.Iteration)
        /**
         * Перемешивает порядок ключей перед измерением.
         */
        public void setupIteration() {
            long iterSeed = seed + iteration++;
            shuffle(presentKeys, iterSeed);
            shuffle(absentKeys, iterSeed + 1);
            prefault(ht, presentKeys);
            p = 0;
        }

        @TearDown(Level.Trial)
        /**
         * Освобождает ресурсы после завершения измерений
         */
        public void tearDown() {
            if (ht != null) {
                ht.close();
            }
        }

        /**
         * Возвращает следующий ключ, который есть в таблице
         */
        int nextPresent() {
            int k = presentKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }

        /**
         * Возвращает следующий ключ, которого нет в таблице
         */
        int nextAbsent() {
            int k = absentKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }
    }

    @State(Scope.Thread)
    public static class WriteState {
        @Param({"8", "16"})
        public int bucketCapacity;

        @Param({"100000", "200000"})
        public int n;

        @Param({"42"})
        public long seed;

        ExtendableHashTable ht;
        int[] baseKeys;
        int[] extraKeys;
        int[] preallocKeys;
        int p;
        int iteration;

        @Setup(Level.Trial)
        /**
         * Создаёт таблицу и заранее заполняет наборы ключей (removeThenPut, updateExisting, putThenRemove)
         */
        public void setupTrial() {
            ht = new ExtendableHashTable(bucketCapacity);

            baseKeys = new int[n];
            extraKeys = new int[n];
            preallocKeys = new int[n];

            for (int i = 0; i < n; i++) {
                baseKeys[i] = i;
                extraKeys[i] = i + n;
                preallocKeys[i] = i + (n << 1);
            }

            for (int key : preallocKeys) {
                ht.put(key, key);
            }
            for (int key : baseKeys) {
                ht.put(key, key);
            }

            prefault(ht, preallocKeys);
            prefault(ht, baseKeys);
            p = 0;
            iteration = 0;
        }

        @Setup(Level.Iteration)
        /**
         * Меняет порядок ключей перед измерением.
         */
        public void setupIteration() {
            long iterSeed = seed + iteration++;
            shuffle(baseKeys, iterSeed);
            shuffle(extraKeys, iterSeed + 1);
            prefault(ht, baseKeys);
            p = 0;
        }

        @TearDown(Level.Trial)
        /**
         * Закрывает таблицу после завершения измерений.
         */
        public void tearDownTrial() {
            if (ht != null) {
                ht.close();
            }
        }

        /**
         * Возвращает следующий ключ, существующий в таблице.
         */
        int nextBase() {
            int k = baseKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }

        /**
         * Возвращает следующий ключ, отсутствующий в таблице.
         */
        int nextExtra() {
            int k = extraKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }
    }

    @State(Scope.Thread)
    public static class BuildState {
        @Param({"8", "16"})
        public int bucketCapacity;

        @Param({"200000", "350000"})
        public int n;

        @Param({"42"})
        public long seed;

        int[] keys;
        int iteration;

        @Setup(Level.Trial)
        /**
         * Подготавливает массив ключей для замера построения таблицы
         */
        public void setupTrial() {
            keys = new int[n];
            for (int i = 0; i < n; i++) {
                keys[i] = i;
            }
            iteration = 0;
        }

        @Setup(Level.Iteration)
        /**
         * Перемешивает ключи перед измерением построения
         */
        public void setupIteration() {
            shuffle(keys, seed + iteration++);
        }
    }

    @Benchmark
    /**
     * Замеряет чтение по ключу, который есть в таблице
     */
    public void getHit(ReadState s, Blackhole bh) {
        bh.consume(s.ht.get(s.nextPresent()));
    }

    @Benchmark
    /**
     * Замеряет чтение по ключу, которого нет
     */
    public void getMiss(ReadState s, Blackhole bh) {
        bh.consume(s.ht.get(s.nextAbsent()));
    }

    @Benchmark
    /**
     * Замеряет вставку и удаление ключа
     */
    public void putThenRemove(WriteState s) {
        int k = s.nextExtra();
        s.ht.put(k, k);
        s.ht.remove(k);
    }

    @Benchmark
    /**
     * Замеряет удаление и вставку ключа
     */
    public void removeThenPut(WriteState s) {
        int k = s.nextBase();
        s.ht.remove(k);
        s.ht.put(k, k);
    }

    @Benchmark
    /**
     * Замеряет обновление значения ключа
     */
    public void updateExisting(WriteState s, Blackhole bh) {
        int k = s.nextBase();
        int v = ~k;
        s.ht.put(k, v);
        bh.consume(s.ht.get(k));
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    /**
     * Замеряет построение таблицы
     */
    public int buildFromScratch(BuildState s, Blackhole bh) {
        try (ExtendableHashTable ht = new ExtendableHashTable(s.bucketCapacity)) {
            for (int key : s.keys) {
                ht.put(key, key);
            }
            prefault(ht, s.keys);
            bh.consume(ht.get(s.keys[s.n / 2]));
            return s.n;
        }
    }

    /**
     * Предварительное чтение
     */
    private static void prefault(ExtendableHashTable ht, int[] keys) {
        for (int i = 0; i < keys.length; i += PREFETCH_STRIDE) {
            ht.get(keys[i]);
        }
    }

    /**
     * Перемешивает массив чисел
     */
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
        withInclude[forwarded.length] = ExtendableHashTableBenchmark.class.getName() + ".*";
        org.openjdk.jmh.Main.main(withInclude);
    }
}
