package perfectHashing;

import java.util.SplittableRandom;

public class PerfectHashTable<V> {
    private static final int P = 2_147_483_647;
    private static final int MEMORY_FACTOR = 4;

    private final int n;
    private final HashFunction h1;
    private final SecondaryTable<V>[] level2;

    private PerfectHashTable(int n, HashFunction h1, SecondaryTable<V>[] level2) {
        this.n = n;
        this.h1 = h1;
        this.level2 = level2;
    }

    public static <V> PerfectHashTable<V> build(int[] keys, V[] values, long seed) {
        if (keys == null || values == null) {
            throw new IllegalArgumentException("keys and values must not be null");
        }
        if (keys.length != values.length) {
            throw new IllegalArgumentException("keys and values must have the same length");
        }

        int n = keys.length;
        int m = Math.max(1, n);
        SplittableRandom rnd = new SplittableRandom(seed);

        HashFunction h1;
        int[][] keysByBucket;
        Object[][] valuesByBucket;

        while (true) {
            h1 = HashFunction.random(rnd);

            int[] sizes = new int[m];
            for (int key : keys) {
                sizes[h1.mod(key, m)]++;
            }

            long sumSquares = 0L;
            for (int s : sizes) {
                sumSquares += (long) s * s;
            }
            if (sumSquares > (long) MEMORY_FACTOR * n) {
                continue;
            }

            keysByBucket = new int[m][];
            valuesByBucket = new Object[m][];
            int[] pos = new int[m];

            for (int j = 0; j < m; j++) {
                keysByBucket[j] = new int[sizes[j]];
                valuesByBucket[j] = new Object[sizes[j]];
            }

            for (int i = 0; i < n; i++) {
                int bucket = h1.mod(keys[i], m);
                int p = pos[bucket]++;
                keysByBucket[bucket][p] = keys[i];
                valuesByBucket[bucket][p] = values[i];
            }

            break;
        }

        @SuppressWarnings("unchecked")
        SecondaryTable<V>[] level2 = (SecondaryTable<V>[]) new SecondaryTable<?>[m];

        for (int j = 0; j < m; j++) {
            level2[j] = SecondaryTable.buildForBucket(keysByBucket[j], valuesByBucket[j], rnd);
        }

        return new PerfectHashTable<>(n, h1, level2);
    }

    public V get(int key) {
        int bucket = h1.mod(key, level2.length);
        return level2[bucket].get(key);
    }

    public boolean containsKey(int key) {
        int bucket = h1.mod(key, level2.length);
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
            long x = ((long) a * key + b) % P;
            if (x < 0) {
                x += P;
            }
            return (int) (x % mod);
        }

        static HashFunction random(SplittableRandom rnd) {
            return new HashFunction(rnd.nextInt(1, P), rnd.nextInt(0, P));
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

                boolean ok = true;
                for (int i = 0; i < nj; i++) {
                    int key = bucketKeys[i];
                    int pos = h2.mod(key, size);

                    if (used[pos] && keys[pos] != key) {
                        ok = false;
                        break;
                    }

                    used[pos] = true;
                    keys[pos] = key;
                    values[pos] = bucketValues[i];
                }

                if (ok) {
                    return new SecondaryTable<>(size, h2, keys, used, values);
                }
            }
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
}
