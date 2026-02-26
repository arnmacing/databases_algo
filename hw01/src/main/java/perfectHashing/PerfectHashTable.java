package perfectHashing;

import java.util.SplittableRandom;

public class PerfectHashTable<V> {
    private static final int P = 2_147_483_647;

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
        // TODO:
        // 1) validate входа: null, длины, n
        // 2) выбрать m = max(1, n)
        // 3) подобрать h1 (несколько попыток) так, чтобы sum(nj^2) было "адекватным"
        //    (классика: пытаться, пока sum(nj^2) <= 4n или <= C*n)
        // 4) для каждого bucket-а построить SecondaryTable:
        //      - size = nj^2
        //      - подобрать h2 без коллизий
        throw new UnsupportedOperationException("TODO: implement build()");
    }

    public V get(int key) {
        // TODO:
        // 1) bucket = h1.mod(key, m)
        // 2) st = level2[bucket]
        // 3) если st пустой -> null
        // 4) pos = st.h2.mod(key, st.size)
        // 5) if st.used[pos] && st.keys[pos] == key -> st.values[pos] else null
        return null;
    }

    public boolean containsKey(int key) {
        return get(key) != null;
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
            // TODO:
            // long x = ( ( (long)a * (long)key ) + (long)b ) % P;
            // if (x < 0) x += P;  // на случай отрицательных key
            // return (int)(x % mod);
            throw new UnsupportedOperationException("TODO: implement mod()");
        }

        static HashFunction random(SplittableRandom rnd) {
            // TODO:
            // int a = rnd.nextInt(1, P); // 1..P-1
            // int b = rnd.nextInt(0, P); // 0..P-1
            throw new UnsupportedOperationException("TODO: implement random()");
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

        static <V> SecondaryTable<V> buildForBucket(int[] bucketKeys, Object[] bucketValues, SplittableRandom rnd) {
            // TODO:
            // 1) nj = bucketKeys.length
            // 2) if nj == 0 -> empty()
            // 3) size = nj*nj
            // 4) loop:
            //      h2 = HashFunction.random(rnd)
            //      попытаться разместить все ключи:
            //        - если коллизия -> continue
            //        - иначе -> готово
            throw new UnsupportedOperationException("TODO: implement buildForBucket()");
        }

        boolean tryPlaceAll(int[] bucketKeys, Object[] bucketValues) {
            // TODO:
            // Arrays.fill(used, false);
            // for i:
            //   pos = h2.mod(bucketKeys[i], size)
            //   if used[pos] -> fail
            //   used[pos]=true; keys[pos]=bucketKeys[i]; values[pos]=bucketValues[i]
            return false;
        }

        @SuppressWarnings("unchecked")
        V get(int key) {
            return null;
        }
    }

    private static void validateInput(int[] keys, Object[] values) {
        // TODO: null checks, length checks, возможно проверка уникальности ключей (O(n log n) / HashSet)
        throw new UnsupportedOperationException("TODO: implement validateInput()");
    }

    private static int chooseM(int n) {
        // TODO: стандартно m = n, но при n=0 -> 1
        return Math.max(1, n);
    }

    private static Buckets splitToBuckets(int[] keys, Object[] values, HashFunction h1, int m) {
        // TODO:
        // 1) посчитать sizes[m]
        // 2) выделить массивы под каждый bucket нужного размера
        // 3) второй проход — заполнить
        throw new UnsupportedOperationException("TODO: implement splitToBuckets()");
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
