package perfectHashing;

import java.util.SplittableRandom;

public class PerfectHashTable<V> {
    private static final int P = 2_147_483_647;
    private static final long MAX_SUM_SQUARES_FACTOR = 4L;
    private static final int H1_CANDIDATES_PER_ROUND = 16;

    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final boolean[] EMPTY_BOOL_ARRAY = new boolean[0];
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private final int n;
    private final HashFunction h1;
    private final SecondaryTable<V>[] level2;
    private final long secondaryTableSize;
    private final int primaryCollisionCount;

    private PerfectHashTable(
            int n,
            HashFunction h1,
            SecondaryTable<V>[] level2,
            long secondaryTableSize,
            int primaryCollisionCount
    ) {
        this.n = n;
        this.h1 = h1;
        this.level2 = level2;
        this.secondaryTableSize = secondaryTableSize;
        this.primaryCollisionCount = primaryCollisionCount;
    }

    /**
     * Строит двухуровневую таблицу без коллизий внутри бакетов второго уровня
     */
    public static <V> PerfectHashTable<V> build(int[] keys, V[] values, long seed) {
        int n = keys.length;
        int m = Math.max(1, n);
        SplittableRandom rnd = new SplittableRandom(seed);

        FirstLevelPlan plan = chooseBestFirstLevel(keys, m, rnd);

        int[][] keysByBucket = new int[m][];
        Object[][] valuesByBucket = new Object[m][];
        int[] pos = new int[m];

        for (int j = 0; j < m; j++) {
            int size = plan.sizes[j];
            keysByBucket[j] = (size == 0) ? EMPTY_INT_ARRAY : new int[size];
            valuesByBucket[j] = (size == 0) ? EMPTY_OBJECT_ARRAY : new Object[size];
        }

        for (int i = 0; i < n; i++) {
            int bucket = plan.h1.mod(keys[i], m);
            int p = pos[bucket]++;
            keysByBucket[bucket][p] = keys[i];
            valuesByBucket[bucket][p] = values[i];
        }

        int primaryCollisionCount = 0;
        for (int size : plan.sizes) {
            if (size > 1) {
                primaryCollisionCount += size - 1;
            }
        }

        @SuppressWarnings("unchecked")
        SecondaryTable<V>[] level2 = (SecondaryTable<V>[]) new SecondaryTable<?>[m];

        long secondaryTableSize = 0L;
        for (int j = 0; j < m; j++) {
            level2[j] = SecondaryTable.buildForBucket(keysByBucket[j], valuesByBucket[j], rnd);
            secondaryTableSize += level2[j].size;
        }

        return new PerfectHashTable<>(
                n,
                plan.h1,
                level2,
                secondaryTableSize,
                primaryCollisionCount
        );
    }

    /**
     * Возвращает значение по ключу
     */
    public V get(int key) {
        int bucket = h1.mod(key, level2.length);
        return level2[bucket].get(key);
    }

    /**
     * Проверяет наличие ключа
     */
    public boolean containsKey(int key) {
        int bucket = h1.mod(key, level2.length);
        return level2[bucket].containsKey(key);
    }

    /**
     * Возвращает число пар ключ-значение
     */
    public int size() {
        return n;
    }

    /**
     * Суммарный размер таблиц второго уровня (сумма nj^2)
     */
    public long secondaryTableSize() {
        return secondaryTableSize;
    }

    /**
     * Число коллизий первого уровня: сумма (size(bucket) - 1) для непустых бакетов
     */
    public int primaryCollisionCount() {
        return primaryCollisionCount;
    }

    /**
     * Коэффициент расширения относительно числа ключей
     */
    public double expansionFactor() {
        if (n == 0) {
            return 0.0;
        }
        return (double) secondaryTableSize / (double) n;
    }

    private static FirstLevelPlan chooseBestFirstLevel(
            int[] keys,
            int m,
            SplittableRandom rnd
    ) {
        long limit = MAX_SUM_SQUARES_FACTOR * keys.length;

        while (true) {
            FirstLevelPlan best = null;

            for (int attempt = 0; attempt < H1_CANDIDATES_PER_ROUND; attempt++) {
                HashFunction h1 = HashFunction.random(rnd);
                int[] sizes = new int[m];

                for (int key : keys) {
                    sizes[h1.mod(key, m)]++;
                }

                long sumSquares = 0L;
                int maxBucketSize = 0;
                for (int size : sizes) {
                    sumSquares += (long) size * size;
                    if (size > maxBucketSize) {
                        maxBucketSize = size;
                    }
                }

                if (sumSquares > limit) {
                    continue;
                }

                if (best == null
                        || sumSquares < best.sumSquares
                        || (sumSquares == best.sumSquares
                        && maxBucketSize < best.maxBucketSize)) {
                    best = new FirstLevelPlan(h1, sizes, sumSquares, maxBucketSize);
                }
            }

            if (best != null) {
                return best;
            }
        }
    }

    private static final class FirstLevelPlan {
        final HashFunction h1;
        final int[] sizes;
        final long sumSquares;
        final int maxBucketSize;

        FirstLevelPlan(
                HashFunction h1,
                int[] sizes,
                long sumSquares,
                int maxBucketSize
        ) {
            this.h1 = h1;
            this.sizes = sizes;
            this.sumSquares = sumSquares;
            this.maxBucketSize = maxBucketSize;
        }
    }

    static final class HashFunction {
        final int a;
        final int b;

        HashFunction(int a, int b) {
            this.a = a;
            this.b = b;
        }

        /**
         * Переводит ключ в номер бакета по модулю заданного размера
         */
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

        private SecondaryTable(
                int size,
                HashFunction h2,
                int[] keys,
                boolean[] used,
                Object[] values
        ) {
            this.size = size;
            this.h2 = h2;
            this.keys = keys;
            this.used = used;
            this.values = values;
        }

        /**
         * Возвращает пустую таблицу
         */
        static <V> SecondaryTable<V> empty() {
            return new SecondaryTable<>(
                    0,
                    new HashFunction(1, 0),
                    EMPTY_INT_ARRAY,
                    EMPTY_BOOL_ARRAY,
                    EMPTY_OBJECT_ARRAY
            );
        }

        /**
         * Возвращает таблицу для одного ключа без подбора второй функции
         */
        static <V> SecondaryTable<V> singleton(int key, Object value) {
            int[] keys = new int[]{key};
            boolean[] used = new boolean[]{true};
            Object[] values = new Object[]{value};

            return new SecondaryTable<>(1, new HashFunction(1, 0), keys, used, values);
        }

        /**
         * Строит таблицу второго уровня для бакета
         */
        static <V> SecondaryTable<V> buildForBucket(
                int[] bucketKeys,
                Object[] bucketValues,
                SplittableRandom rnd
        ) {
            int nj = bucketKeys.length;

            if (nj == 0) {
                return empty();
            }

            if (nj == 1) {
                return singleton(bucketKeys[0], bucketValues[0]);
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