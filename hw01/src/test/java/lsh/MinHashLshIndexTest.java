package lsh;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

final class MinHashLshIndexTest {

    @Test
        // близкие по смыслу и оформлению документы попадают в кандидаты
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
        // вероятностный поиск
    void nearDuplicates_shouldMatchFullScanOnSmallSet() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 123L);
        idx.add(1, "кошки любят молоко");
        idx.add(2, "кошки любят молочко");
        idx.add(3, "самолеты летают высоко");
        idx.add(4, "кошки любят молоко!!");
        double th = 0.6;

        List<Pair> lsh = idx.nearDuplicates(th);
        List<Pair> full = idx.nearDuplicatesFullScan(th);

        // Вероятностный отбор может пропустить часть пар
        // Делаем проверку: все найденные пары должны быть и при полном переборе
        for (Pair p : lsh) {
            assertTrue(containsPair(full, p.left, p.right));
        }
    }

    // Ищет пару в списке без учёта порядка
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
        // защита от повторного использования одного идентификатора документа
    void add_shouldRejectDuplicateDocId() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, 42L);
        idx.add(1, "hello world");
        assertThrows(IllegalArgumentException.class, () -> idx.add(1, "another text"));
    }

    @Test
        // устойчивость к пустому и короткому входу
    void shinglesOf_shouldHandleNullAndShortText() {
        MinHashLshIndex idx = new MinHashLshIndex(10, 64, 8, 42L);
        idx.add(1, null);
        idx.add(2, "short");
    }

    @Test
        // граничный случай меры Жаккара для двух пустых наборов
    void jaccard_shouldReturnOneForBothEmpty() {
        assertEquals(1.0, MinHashLshIndex.jaccard(new int[0], new int[0]), 1e-12);
    }

    @Test
        // структура корректно обрабатывает много похожих записей подряд
    void intList_shouldGrow() {
        MinHashLshIndex idx = new MinHashLshIndex(5, 64, 8, 42L);
        // Подберём тексты так, чтобы много документов попали в один и тот же бакет в одной полосе
        for (int i = 0; i < 10; i++) {
            idx.add(100 + i, "same same same text");
        }
        List<Pair> pairs = idx.nearDuplicates(0.9);
        assertFalse(pairs.isEmpty());
    }

    @Test
        // на случайных данных
    void randomized_dataset_shouldFindPlantedDuplicates() {
        long seed = 2026L;
        java.util.SplittableRandom rnd = new java.util.SplittableRandom(seed);

        MinHashLshIndex idx = new MinHashLshIndex(5, 128, 32, seed);
        double th = 0.95;
        int groups = 30;
        int noise = 50;

        java.util.List<int[]> expectedPairs = new java.util.ArrayList<>();
        int docId = 1;

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

        // полный
        for (int[] p : expectedPairs) {
            assertTrue(containsPair(full, p[0], p[1]));
        }

        // вероятностный
        for (int[] p : expectedPairs) {
            assertTrue(containsPair(lsh, p[0], p[1]));
        }

        // 3) что нашёл вероятностный отбор, должно быть в полном переборе
        for (Pair p : lsh) {
            assertTrue(containsPair(full, p.left, p.right));
        }
    }

    // случайный текст
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

    // изменение регистра, знаков и пробелов
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
