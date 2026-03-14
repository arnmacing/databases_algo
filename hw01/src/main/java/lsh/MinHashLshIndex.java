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
    private final List<Map<Integer, List<Integer>>> tables;
    private final Map<Integer, int[]> docShingles;

    public MinHashLshIndex(int shingleSize, int signatureSize, int bands, long seed) {
        this.shingleSize = shingleSize;
        this.signatureSize = signatureSize;
        this.bands = bands;
        this.rows = signatureSize / bands;

        this.a = new int[signatureSize];
        this.b = new int[signatureSize];
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = 0; i < signatureSize; i++) {
            a[i] = rnd.nextInt(1, P);
            b[i] = rnd.nextInt(0, P);
        }

        this.tables = new ArrayList<>(bands);
        for (int i = 0; i < bands; i++) {
            tables.add(new HashMap<>());
        }

        this.docShingles = new HashMap<>();
    }

    public void add(int docId, String text) {
        if (docShingles.containsKey(docId)) {
            throw new IllegalArgumentException("docId already exists: " + docId);
        }

        int[] shingles = shinglesOf(text);
        int[] sig = signatureOf(shingles);
        docShingles.put(docId, shingles);

        for (int band = 0; band < bands; band++) {
            int key = bandKey(sig, band);
            tables.get(band).computeIfAbsent(key, k -> new ArrayList<>()).add(docId);
        }
    }

    public Set<Integer> candidates(String text) {
        int[] sig = signatureOf(shinglesOf(text));
        Set<Integer> result = new HashSet<>();

        for (int band = 0; band < bands; band++) {
            List<Integer> ids = tables.get(band).get(bandKey(sig, band));
            if (ids != null) {
                result.addAll(ids);
            }
        }

        return result;
    }

    public List<Pair> nearDuplicates(double threshold) {
        Set<Long> seenPairs = new HashSet<>();
        List<Pair> result = new ArrayList<>();

        for (int band = 0; band < bands; band++) {
            for (List<Integer> bucket : tables.get(band).values()) {
                int size = bucket.size();
                if (size < 2) {
                    continue;
                }

                for (int i = 0; i < size; i++) {
                    for (int j = i + 1; j < size; j++) {
                        int x = bucket.get(i);
                        int y = bucket.get(j);
                        long packed = packPair(x, y);
                        if (!seenPairs.add(packed)) {
                            continue;
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

    public List<Pair> nearDuplicatesFullScan(double threshold) {
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
            return new int[]{s.hashCode()};
        }

        Set<Integer> set = new HashSet<>();
        for (int i = 0; i + shingleSize <= s.length(); i++) {
            set.add(s.substring(i, i + shingleSize).hashCode());
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
        long v = ((long) a * x + b) % P;
        if (v < 0) {
            v += P;
        }
        return (int) v;
    }

    private int bandKey(int[] sig, int band) {
        int start = band * rows;
        int h = 1;
        for (int i = 0; i < rows; i++) {
            h = 31 * h + sig[start + i];
        }
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
        return (union == 0) ? 1.0 : (double) inter / union;
    }

    private static long packPair(int x, int y) {
        int a = Math.min(x, y);
        int b = Math.max(x, y);
        return (((long) a) << 32) | Integer.toUnsignedLong(b);
    }
}
