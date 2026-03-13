package lsh;

import java.util.*;

public final class MinHashLshIndex {

    private static final int P = 2_147_483_647;
    private final int shingleSize;
    private final int signatureSize;
    private final int bands;
    private final int rows;
    private final int[] a;
    private final int[] b;
    private final List<Map<Long, IntList>> tables;
    private final Map<Integer, int[]> docShingles;
    private final Map<Integer, int[]> docSignature;

    private final SplittableRandom rnd;

    public MinHashLshIndex(int shingleSize, int signatureSize, int bands, long seed) {
        if (shingleSize <= 0) {
            throw new IllegalArgumentException("shingleSize must be > 0");
        }
        if (signatureSize <= 0) {
            throw new IllegalArgumentException("signatureSize must be > 0");
        }
        if (bands <= 0) {
            throw new IllegalArgumentException("bands must be > 0");
        }
        if (signatureSize % bands != 0) {
            throw new IllegalArgumentException("signatureSize must be divisible by bands");
        }

        this.shingleSize = shingleSize;
        this.signatureSize = signatureSize;
        this.bands = bands;
        this.rows = signatureSize / bands;
        this.rnd = new SplittableRandom(seed);
        this.a = new int[signatureSize];
        this.b = new int[signatureSize];
        for (int i = 0; i < signatureSize; i++) {
            a[i] = rnd.nextInt(1, P);
            b[i] = rnd.nextInt(0, P);
        }

        this.tables = new ArrayList<>(bands);
        for (int i = 0; i < bands; i++) {
            tables.add(new HashMap<>());
        }

        this.docShingles = new HashMap<>();
        this.docSignature = new HashMap<>();
    }

    /**
     * Добавить документ в индекс.
     */
    public void add(int docId, String text) {
        if (docShingles.containsKey(docId)) {
            throw new IllegalArgumentException("docId already exists: " + docId);
        }
        int[] shingles = shinglesOf(text);
        int[] sig = signatureOf(shingles);

        docShingles.put(docId, shingles);
        docSignature.put(docId, sig);

        for (int band = 0; band < bands; band++) {
            long key = bandKey(sig, band);
            Map<Long, IntList> table = tables.get(band);
            table.computeIfAbsent(key, k -> new IntList()).add(docId);
        }
    }

    /**
     * Получить кандидатов для текста.
     */
    public Set<Integer> candidates(String text) {
        int[] shingles = shinglesOf(text);
        int[] sig = signatureOf(shingles);

        Set<Integer> result = new HashSet<>();
        for (int band = 0; band < bands; band++) {
            long key = bandKey(sig, band);
            IntList ids = tables.get(band).get(key);
            if (ids != null) {
                ids.addTo(result);
            }
        }
        return result;
    }

    /**
     * Поиск дубликатов среди уже добавленных документов.
     */
    public List<Pair> nearDuplicates(double threshold) {
        if (threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("threshold must be in [0,1]");
        }

        // по каждому бакету внутри каждой полосы
        Set<Long> seenPairs = new HashSet<>();
        List<Pair> result = new ArrayList<>();

        for (int band = 0; band < bands; band++) {
            for (IntList bucket : tables.get(band).values()) {
                int size = bucket.size();
                if (size < 2) {
                    continue;
                }
                // генерим пары внутри бакета
                int[] ids = bucket.toArray();
                for (int i = 0; i < ids.length; i++) {
                    for (int j = i + 1; j < ids.length; j++) {
                        int x = ids[i];
                        int y = ids[j];
                        long packed = packPair(x, y);
                        if (!seenPairs.add(packed)) {
                            continue; // уже проверяли
                        }
                        double jac = jaccard(docShingles.get(x), docShingles.get(y));
                        if (jac >= threshold) {
                            result.add(new Pair(x, y, jac));
                        }
                    }
                }
            }
        }

        result.sort(Comparator.comparingDouble((Pair p) -> -p.similarity));
        return result;
    }

    /**
     * точный Jaccard для тестов
     */
    public List<Pair> nearDuplicatesFullScan(double threshold) {
        if (threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("threshold must be in [0,1]");
        }

        List<Integer> ids = new ArrayList<>(docShingles.keySet());
        Collections.sort(ids);

        List<Pair> result = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            for (int j = i + 1; j < ids.size(); j++) {
                int x = ids.get(i);
                int y = ids.get(j);
                double jac = jaccard(docShingles.get(x), docShingles.get(y));
                if (jac >= threshold) {
                    result.add(new Pair(x, y, jac));
                }
            }
        }

        result.sort(Comparator.comparingDouble((Pair p) -> -p.similarity));
        return result;
    }

    private int[] shinglesOf(String text) {
        String s = normalize(text);
        if (s.length() < shingleSize) {
            return new int[]{hash32(s)};
        }

        Set<Integer> set = new HashSet<>();
        for (int i = 0; i + shingleSize <= s.length(); i++) {
            String sh = s.substring(i, i + shingleSize);
            set.add(hash32(sh));
        }

        int[] arr = new int[set.size()];
        int k = 0;
        for (int v : set) {
            arr[k++] = v;
        }
        Arrays.sort(arr);
        return arr;
    }

    private static String normalize(String text) {
        if (text == null) {
            return "";
        }
        String s = text.toLowerCase(Locale.ROOT);
        s = s.replaceAll("[^\\p{L}\\p{N}\\s]+", " ");
        s = s.replaceAll("\\s+", " ").trim();
        return s;
    }

    private int[] signatureOf(int[] shinglesSortedUnique) {
        int[] sig = new int[signatureSize];
        Arrays.fill(sig, Integer.MAX_VALUE);

        for (int x : shinglesSortedUnique) {
            for (int i = 0; i < signatureSize; i++) {
                int hx = hashUniversal(a[i], b[i], x);
                if (hx < sig[i]) {
                    sig[i] = hx;
                }
            }
        }
        return sig;
    }

    private static int hashUniversal(int a, int b, int x) {
        long v = ((long) a * (long) x + (long) b) % (long) P;
        if (v < 0) {
            v += P;
        }
        return (int) v;
    }

    private long bandKey(int[] sig, int band) {
        int start = band * rows;

        // FNV-1a 64-bit
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < rows; i++) {
            int v = sig[start + i];
            h ^= (v & 0xffffffffL);
            h *= 0x100000001b3L;
        }
        h ^= (long) band * 0x9e3779b97f4a7c15L;
        return h;
    }

    static double jaccard(int[] a, int[] b) {
        int i = 0;
        int j = 0;
        int inter = 0;
        int union = 0;

        while (i < a.length && j < b.length) {
            int x = a[i];
            int y = b[j];
            if (x == y) {
                inter++;
                union++;
                i++;
                j++;
            } else if (x < y) {
                union++;
                i++;
            } else {
                union++;
                j++;
            }
        }
        union += (a.length - i) + (b.length - j);

        if (union == 0) {
            return 1.0; // оба пустые
        }
        return (double) inter / (double) union;
    }

    private static int hash32(String s) {
        // FNV-1a
        int h = 0x811c9dc5;
        for (int i = 0; i < s.length(); i++) {
            h ^= s.charAt(i);
            h *= 0x01000193;
        }
        return h;
    }

    private static long packPair(int x, int y) {
        int a = Math.min(x, y);
        int b = Math.max(x, y);
        return ((long) a << 32) ^ (b & 0xffffffffL);
    }

    public static final class Pair {
        public final int left;
        public final int right;
        public final double similarity;

        public Pair(int left, int right, double similarity) {
            this.left = left;
            this.right = right;
            this.similarity = similarity;
        }

        @Override
        public String toString() {
            return left + " ~ " + right + " (J=" + similarity + ")";
        }
    }

    static final class IntList {
        private int[] data = new int[4];
        private int size = 0;

        void add(int v) {
            if (size == data.length) {
                data = Arrays.copyOf(data, data.length * 2);
            }
            data[size++] = v;
        }

        int size() {
            return size;
        }

        int[] toArray() {
            return Arrays.copyOf(data, size);
        }

        void addTo(Set<Integer> out) {
            for (int i = 0; i < size; i++) {
                out.add(data[i]);
            }
        }
    }
}