package perfectHashing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;

public class PerfectHashTable<V> {
    private static final int P = 2_147_483_647;
    private static final int MEMORY_FACTOR = 4;
    private final int n;
    private final int m;
    private final HashFunction h1;
    private final SecondaryTable<V>[] level2;

    private PerfectHashTable(int n, int m, HashFunction h1, SecondaryTable<V>[] level2) {
        this.n = n;
        this.m = m;
        this.h1 = h1;
        this.level2 = level2;
    }

    public static <V> PerfectHashTable<V> build(int[] keys, V[] values, long seed) {
        // 1) validate входа: null, длины, n
        // 2) выбрать m = max(1, n)
        // 3) подобрать h1 так, пока sum(nj^2) <= 4n
        // 4) для каждого bucket-а построить SecondaryTable: size = nj^2,  подобрать h2 без коллизий
        validateInput(keys, values);

        int n = keys.length;
        int m = chooseM(n);

        if (n == 0) {
            @SuppressWarnings("unchecked")
            SecondaryTable<V>[] emptyLevel2 = (SecondaryTable<V>[]) new SecondaryTable<?>[m];
            Arrays.fill(emptyLevel2, SecondaryTable.empty());
            return new PerfectHashTable<>(0, m, new HashFunction(1, 0), emptyLevel2);
        }

        SplittableRandom random = new SplittableRandom(seed);

        HashFunction h1;
        Buckets buckets;

        while (true) {
            h1 = HashFunction.random(random);
            buckets = splitToBuckets(keys, values, h1, m);

            long sumSquares = 0L;
            for (int j = 0; j < m; j++) {
                int nj = buckets.keysByBucket[j].length;
                sumSquares += (long) nj * (long) nj;
            }

            if (sumSquares <= (long) MEMORY_FACTOR * (long) n) {
                break;
            }
        }

        @SuppressWarnings("unchecked")
        SecondaryTable<V>[] level2 = (SecondaryTable<V>[]) new SecondaryTable<?>[m];

        for (int j = 0; j < m; j++) {
            int[] bKeys = buckets.keysByBucket[j];
            Object[] bValues = buckets.valuesByBucket[j];
            level2[j] = SecondaryTable.buildForBucket(bKeys, bValues, random);
        }

        return new PerfectHashTable<>(n, m, h1, level2);
    }

    public V get(int key) {
        int bucket = h1.mod(key, m);
        return level2[bucket].get(key);
    }

    public boolean containsKey(int key) {
        int bucket = h1.mod(key, m);
        return level2[bucket].containsKey(key);
    }

    public int size() {
        return n;
    }

    static final class HashFunction {
        final int a;
        final int b;

        HashFunction(int a, int b) {
            this.a = a;
            this.b = b;
        }

        int mod(int key, int mod) {
            long x = ((long) a * (long) key + (long) b) % (long) P;
            if (x < 0) {
                x += P;
            }
            return (int) (x % (long) mod);
        }

        static HashFunction random(SplittableRandom rnd) {
            int a = rnd.nextInt(1, P);
            int b = rnd.nextInt(0, P);
            return new HashFunction(a, b);
        }
    }

    static final class SecondaryTable<V> {
        final int size;
        final HashFunction h2;
        final int[] keys;
        final boolean[] used;
        final Object[] values;

        private SecondaryTable(int size, HashFunction h2, int[] keys, boolean[] used, Object[] values) {
            this.size = size;
            this.h2 = h2;
            this.keys = keys;
            this.used = used;
            this.values = values;
        }

        static <V> SecondaryTable<V> empty() {
            return new SecondaryTable<>(0, new HashFunction(1, 0), new int[0], new boolean[0], new Object[0]);
        }

        static <V> SecondaryTable<V> buildForBucket(int[] bucketKeys, Object[] bucketValues, SplittableRandom rnd) {
            int nj = bucketKeys.length;
            if (nj == 0) {
                return empty();
            }

            int size = nj * nj;

            while (true) {
                HashFunction h2 = HashFunction.random(rnd);
                int[] keys = new int[size];
                boolean[] used = new boolean[size];
                Object[] values = new Object[size];

                SecondaryTable<V> table = new SecondaryTable<>(size, h2, keys, used, values);

                if (table.tryPlaceAll(bucketKeys, bucketValues)) {
                    return table;
                }
            }
        }

        boolean tryPlaceAll(int[] bucketKeys, Object[] bucketValues) {
            Arrays.fill(used, false);
            for (int i = 0; i < bucketKeys.length; i++) {
                int key = bucketKeys[i];
                int pos = h2.mod(key, size);

                if (used[pos]) {
                    return false;
                }

                used[pos] = true;
                keys[pos] = key;
                values[pos] = bucketValues[i];
            }
            return true;
        }

        @SuppressWarnings("unchecked")
        V get(int key) {
            if (size == 0) {
                return null;
            }

            int pos = h2.mod(key, size);
            if (used[pos] && keys[pos] == key) {
                return (V) values[pos];
            }
            return null;
        }

        boolean containsKey(int key) {
            if (size == 0) {
                return false;
            }
            int pos = h2.mod(key, size);
            return used[pos] && keys[pos] == key;
        }
    }

    private static void validateInput(int[] keys, Object[] values) {
        if (keys == null) {
            throw new IllegalArgumentException("keys must not be null");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        if (keys.length != values.length) {
            throw new IllegalArgumentException("keys and values must have the same length");
        }

        Set<Integer> seen = new HashSet<>(Math.max(16, keys.length * 2));
        for (int key : keys) {
            if (!seen.add(key)) {
                throw new IllegalArgumentException("duplicate key: " + key);
            }
        }
    }

    private static int chooseM(int n) {
        // m = n, но при n=0 -> 1
        return Math.max(1, n);
    }

    private static Buckets splitToBuckets(int[] keys, Object[] values, HashFunction h1, int m) {
        // 1) посчитать sizes[m]
        // 2) выделить массивы под каждый bucket нужного размера
        // 3) второй проход - заполнить

        int[] sizes = new int[m];

        for (int key : keys) {
            int bucket = h1.mod(key, m);
            sizes[bucket]++;
        }

        int[][] keysByBucket = new int[m][];
        Object[][] valuesByBucket = new Object[m][];
        int[] pos = new int[m];

        for (int j = 0; j < m; j++) {
            keysByBucket[j] = new int[sizes[j]];
            valuesByBucket[j] = new Object[sizes[j]];
        }

        for (int i = 0; i < keys.length; i++) {
            int key = keys[i];
            int bucket = h1.mod(key, m);
            int p = pos[bucket]++;
            keysByBucket[bucket][p] = key;
            valuesByBucket[bucket][p] = values[i];
        }

        return new Buckets(keysByBucket, valuesByBucket);
    }

    static final class Buckets {
        final int[][] keysByBucket;
        final Object[][] valuesByBucket;

        Buckets(int[][] keysByBucket, Object[][] valuesByBucket) {
            this.keysByBucket = keysByBucket;
            this.valuesByBucket = valuesByBucket;
        }
    }
}
