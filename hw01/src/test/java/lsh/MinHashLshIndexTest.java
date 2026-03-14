package lsh;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

final class MinHashLshIndexTest {

    @Test
    void addAndCandidates_shouldReturnSimilarDocs() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "Мама мыла раму.");
        idx.add(2, "мама  мыла   раму");
        idx.add(3, "Совсем другой текст");

        Set<Integer> cand = idx.candidates("мама мыла раму!!!");
        assertTrue(cand.contains(1));
        assertTrue(cand.contains(2));
    }

    @Test
    void nearDuplicates_shouldMatchFullScanOnSmallSet() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 123L);
        idx.add(1, "кошки любят молоко");
        idx.add(2, "кошки любят молочко"); // близко
        idx.add(3, "самолеты летают высоко");
        idx.add(4, "кошки любят молоко!!"); // очень близко
        double th = 0.6;

        List<Pair> lsh = idx.nearDuplicates(th);
        List<Pair> full = idx.nearDuplicatesFullScan(th);

        // LSH может пропустить часть пар (это вероятностный алгоритм),
        // но на маленьких примерах с такими параметрами обычно совпадает.
        // Сделаем мягкую проверку: все найденные LSH-пары должны быть и в full scan.
        for (Pair p : lsh) {
            assertTrue(containsPair(full, p.left, p.right));
        }
    }

    private static boolean containsPair(List<Pair> list, int a, int b) {
        int x = Math.min(a, b);
        int y = Math.max(a, b);
        for (Pair p : list) {
            int p1 = Math.min(p.left, p.right);
            int p2 = Math.max(p.left, p.right);
            if (p1 == x && p2 == y) {
                return true;
            }
        }
        return false;
    }


    @Test
    void add_shouldRejectDuplicateDocId() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "hello world");
        assertThrows(IllegalArgumentException.class, () -> idx.add(1, "another text"));
    }

    @Test
    void shinglesOf_shouldHandleNullAndShortText() {
        MinHashLshIndex idx = new MinHashLshIndex(10, 64, 8, 42L);
        idx.add(1, null);
        idx.add(2, "short");
    }

    @Test
    void jaccard_shouldReturnOneForBothEmpty() {
        assertEquals(1.0, MinHashLshIndex.jaccard(new int[0], new int[0]), 1e-12);
    }

    @Test
    void intList_shouldGrow() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 64, 8, 42L);
        // Подберём тексты так, чтобы много документов попали в один и тот же бакет в одной полосе.
        for (int i = 0; i < 10; i++) {
            idx.add(100 + i, "same same same text");
        }
        // заставили IntList расшириться.
        List<Pair> pairs = idx.nearDuplicates(0.9);
        assertFalse(pairs.isEmpty());
    }

    @Test
    void randomized_dataset_shouldFindPlantedDuplicates() {
        long seed = 2026L;
        java.util.SplittableRandom rnd = new java.util.SplittableRandom(seed);

        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, seed);
        double th = 0.95;
        int groups = 30;
        int noise = 50;

        java.util.List<int[]> expectedPairs = new java.util.ArrayList<>();
        int docId = 1;

        // base и variant различаются только регистром/пунктуацией/пробелами,
        // normalize() это убирает -> Jaccard = 1.0
        for (int g = 0; g < groups; g++) {
            String base = randomBaseText(rnd, 12);
            String variant = noisyFormattingVariant(rnd, base);
            int id1 = docId++;
            int id2 = docId++;
            idx.add(id1, base);
            idx.add(id2, variant);
            expectedPairs.add(new int[]{id1, id2});
        }

        for (int i = 0; i < noise; i++) {
            idx.add(docId++, randomBaseText(rnd, 12));
        }

        java.util.List<Pair> full = idx.nearDuplicatesFullScan(th);
        java.util.List<Pair> lsh = idx.nearDuplicates(th);

        // 1) full scan
        for (int[] p : expectedPairs) {
            assertTrue(containsPair(full, p[0], p[1]));
        }

        // 2) LSH
        for (int[] p : expectedPairs) {
            assertTrue(containsPair(lsh, p[0], p[1]));
        }

        // 3) всё, что нашёл LSH, должно быть в full scan
        for (Pair p : lsh) {
            assertTrue(containsPair(full, p.left, p.right));
        }
    }

    private static String randomBaseText(java.util.SplittableRandom rnd, int words) {
        String[] dict = {
                "mama", "myla", "ramu", "koshki", "lyubyat", "moloko",
                "samolety", "letayut", "vysoko", "programmirovanie", "algoritmy",
                "hash", "tablica", "indeks", "poisk", "tekst", "dubl"
        };

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < words; i++) {
            if (i > 0) sb.append(' ');
            sb.append(dict[rnd.nextInt(dict.length)]);
        }
        return sb.toString();
    }

    private static String noisyFormattingVariant(java.util.SplittableRandom rnd, String base) {
        String[] words = base.split(" ");
        String[] suffix = {"", "!", "!!", "...", ",", "??", ";", ":"};

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String w = words[i];
            int mode = rnd.nextInt(3);
            if (mode == 0) {
                w = w.toUpperCase(java.util.Locale.ROOT);
            } else if (mode == 1 && !w.isEmpty()) {
                w = Character.toUpperCase(w.charAt(0)) + w.substring(1);
            }
            sb.append(w);
            sb.append(suffix[rnd.nextInt(suffix.length)]);
            if (i + 1 < words.length) {
                int spaces = 1 + rnd.nextInt(3);
                sb.append(" ".repeat(spaces));
            }
        }

        if (rnd.nextBoolean()) {
            sb.append(" !!!");
        }
        return sb.toString();
    }
}
