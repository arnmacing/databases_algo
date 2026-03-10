package bench;

import extendableHashing.ExtendableHashTable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1) // чета не поняла что Петя сказал про этот параметр
public class Hw01Benchmark {
    @State(Scope.Thread)
    public static class ReadState {

        @Param({"2", "4", "8"})
        public int bucketCapacity;

        @Param({"1000", "10000"})
        public int n;

        @Param({"42"})
        public long seed;

        public ExtendableHashTable ht;
        public int[] presentKeys;
        public int[] absentKeys;

        private int p; // указатель для доступа

        @Setup(Level.Trial)
        public void setup() {
            ht = new ExtendableHashTable(bucketCapacity, seed);

            presentKeys = new int[n];
            absentKeys = new int[n];

            // ключи гарантированно уникальны и детерминированы
            for (int i = 0; i < n; i++) {
                presentKeys[i] = i;
                absentKeys[i] = i + n;
            }

            // перемешаем порядок обращений
            shuffle(presentKeys, seed ^ 0x9E3779B97F4A7C15L);
            shuffle(absentKeys, seed ^ 0xBF58476D1CE4E5B9L);

            for (int k : presentKeys) {
                ht.put(k, k);
            }

            p = 0;
        }

        public int nextPresent() {
            int k = presentKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }

        public int nextAbsent() {
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

        @Param({"2", "4", "8"})
        public int bucketCapacity;

        @Param({"1000", "10000"})
        public int n;

        @Param({"42"})
        public long seed;

        public ExtendableHashTable ht;
        public int[] baseKeys;   // ключи, которые постоянно в таблице
        public int[] extraKeys;  // ключи, которые будем вставлять

        private int p;

        @Setup(Level.Iteration)
        public void setupIteration() {
            ht = new ExtendableHashTable(bucketCapacity, seed);

            baseKeys = new int[n];
            extraKeys = new int[n];

            for (int i = 0; i < n; i++) {
                baseKeys[i] = i;
                extraKeys[i] = i + n;
            }

            shuffle(baseKeys, seed ^ 0xD6E8FEB86659FD93L);
            shuffle(extraKeys, seed ^ 0xA5A3564E27F6D2F1L);

            for (int k : baseKeys) {
                ht.put(k, k);
            }

            p = 0;
        }

        public int nextBase() {
            int k = baseKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }

        public int nextExtra() {
            int k = extraKeys[p];
            p++;
            if (p == n) {
                p = 0;
            }
            return k;
        }
    }

    @Benchmark
    public void getHit(ReadState s, Blackhole bh) {
        Integer v = s.ht.get(s.nextPresent());
        bh.consume(v);
    }

    @Benchmark
    public void getMiss(ReadState s, Blackhole bh) {
        Integer v = s.ht.get(s.nextAbsent());
        bh.consume(v);
    }


    @Benchmark
    public void putThenRemove(WriteState s) {
        int k = s.nextExtra();
        s.ht.put(k, k);
        s.ht.remove(k);
    }

    @Benchmark
    public void removeThenPut(WriteState s) {
        int k = s.nextBase();
        s.ht.remove(k);
        s.ht.put(k, k);
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
        org.openjdk.jmh.Main.main(args);
    }
}
