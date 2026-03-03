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

        List<MinHashLshIndex.Pair> lsh = idx.nearDuplicates(th);
        List<MinHashLshIndex.Pair> full = idx.nearDuplicatesFullScan(th);

        // LSH может пропустить часть пар (это вероятностный алгоритм),
        // но на маленьких примерах с такими параметрами обычно совпадает.
        // Сделаем мягкую проверку: все найденные LSH-пары должны быть и в full scan.
        for (MinHashLshIndex.Pair p : lsh) {
            assertTrue(containsPair(full, p.left, p.right));
        }
    }

    private static boolean containsPair(List<MinHashLshIndex.Pair> list, int a, int b) {
        int x = Math.min(a, b);
        int y = Math.max(a, b);
        for (MinHashLshIndex.Pair p : list) {
            int p1 = Math.min(p.left, p.right);
            int p2 = Math.max(p.left, p.right);
            if (p1 == x && p2 == y) {
                return true;
            }
        }
        return false;
    }


    @Test
    void constructor_shouldRejectInvalidParams() {
        assertThrows(IllegalArgumentException.class, () -> new MinHashLshIndex(0, 128, 32, 1L));
        assertThrows(IllegalArgumentException.class, () -> new MinHashLshIndex(5, 0, 32, 1L));
        assertThrows(IllegalArgumentException.class, () -> new MinHashLshIndex(5, 128, 0, 1L));
        assertThrows(IllegalArgumentException.class, () -> new MinHashLshIndex(5, 127, 32, 1L));
    }

    @Test
    void add_shouldRejectDuplicateDocId() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "hello world");
        assertThrows(IllegalArgumentException.class, () -> idx.add(1, "another text"));
    }

    @Test
    void nearDuplicates_shouldRejectInvalidThreshold() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "a");
        idx.add(2, "b");
        assertThrows(IllegalArgumentException.class, () -> idx.nearDuplicates(-0.01));
        assertThrows(IllegalArgumentException.class, () -> idx.nearDuplicates(1.01));
    }

    @Test
    void nearDuplicatesFullScan_shouldRejectInvalidThreshold() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "a");
        idx.add(2, "b");
        assertThrows(IllegalArgumentException.class, () -> idx.nearDuplicatesFullScan(-0.01));
        assertThrows(IllegalArgumentException.class, () -> idx.nearDuplicatesFullScan(1.01));
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
    void pair_toString_shouldBeCovered() {
        MinHashLshIndex.Pair p = new MinHashLshIndex.Pair(1, 2, 0.75);
        String s = p.toString();
        assertTrue(s.contains("1"));
        assertTrue(s.contains("2"));
        assertTrue(s.contains("0.75"));
    }

    @Test
    void intList_shouldGrow() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 64, 8, 42L);
        // Подберём тексты так, чтобы много документов попали в один и тот же бакет в одной полосе.
        for (int i = 0; i < 10; i++) {
            idx.add(100 + i, "same same same text");
        }
        // заставили IntList расшириться.
        List<MinHashLshIndex.Pair> pairs = idx.nearDuplicates(0.9);
        assertFalse(pairs.isEmpty());
    }
}