package perfectHashing;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.*;

public class PerfectHashTableTest {

    @Test
    void build_shouldRejectNullKeys() {
        Integer[] values = {1, 2, 3};
        Executable act = () -> PerfectHashTable.build(null, values, 42L);
        assertThrows(IllegalArgumentException.class, act);
    }

    @Test
    void build_shouldRejectNullValues() {
        int[] keys = {1, 2, 3};
        Executable act = () -> PerfectHashTable.build(keys, null, 42L);
        assertThrows(IllegalArgumentException.class, act);
    }

    @Test
    void build_shouldRejectLengthMismatch() {
        int[] keys = {1, 2, 3};
        Integer[] values = {10, 20};
        Executable act = () -> PerfectHashTable.build(keys, values, 42L);
        assertThrows(IllegalArgumentException.class, act);
    }

    @Test
    void build_shouldRejectDuplicateKeys() {
        int[] keys = {1, 2, 2, 3};
        Integer[] values = {10, 20, 200, 30};
        Executable act = () -> PerfectHashTable.build(keys, values, 42L);
        assertThrows(IllegalArgumentException.class, act);
    }

    @Test
    void build_emptyInput_shouldWork() {
        int[] keys = {};
        Integer[] values = {};
        PerfectHashTable<Integer> table = PerfectHashTable.build(keys, values, 42L);
        assertEquals(0, table.size());
        assertFalse(table.containsKey(1));
        assertNull(table.get(2));
    }

    @Test
    void get_shouldReturnInsertedValues() {
        int[] keys = {10, 20, 30};
        String[] values = {"A", "B", "C"};

        PerfectHashTable<String> table = PerfectHashTable.build(keys, values, 42L);

        assertEquals(3, table.size());
        assertEquals("A", table.get(10));
        assertEquals("B", table.get(20));
        assertEquals("C", table.get(30));
    }

    @Test
    void get_shouldReturnNullForMissingKey() {
        int[] keys = {10, 20, 30};
        String[] values = {"A", "B", "C"};

        PerfectHashTable<String> table = PerfectHashTable.build(keys, values, 42L);
        assertNull(table.get(100));
        assertFalse(table.containsKey(100));
    }

    @Test
    void get_shouldWorkWithNegativeKeys() {
        int[] keys = {-10, -20, 0, 30};
        String[] values = {"A", "B", "C", "D"};

        PerfectHashTable<String> table = PerfectHashTable.build(keys, values, 42L);

        assertEquals(4, table.size());
        assertEquals("A", table.get(-10));
        assertEquals("B", table.get(-20));
        assertEquals("C", table.get(0));
        assertEquals("D", table.get(30));
        assertNull(table.get(-11));
    }

    @Test
    void containsKey_shouldBeConsistentWithGet() {
        int[] keys = {1, 2, 3};
        Integer[] values = {10, null, 30};

        PerfectHashTable<Integer> table = PerfectHashTable.build(keys, values, 42L);

        assertTrue(table.containsKey(1));
        assertEquals(10, table.get(1));

        assertTrue(table.containsKey(2));
        assertNull(table.get(2));

        assertTrue(table.containsKey(3));
        assertEquals(30, table.get(3));

        assertFalse(table.containsKey(4));
        assertNull(table.get(4));
    }

    @Test
    void containsKey_shouldWorkWithNullValues() {
        int[] keys = {1, 2, 3};
        String[] values = {"A", null, "C"};

        PerfectHashTable<String> table = PerfectHashTable.build(keys, values, 42L);

        assertTrue(table.containsKey(2));
        assertNull(table.get(2));
        assertFalse(table.containsKey(999));
        assertNull(table.get(999));
    }

    @Test
    void randomized_denseKeys_matchesHashMap() {
        long seed = 123L;
        java.util.SplittableRandom rnd = new java.util.SplittableRandom(seed);

        int n = 2000;
        int[] keys = new int[n];
        Integer[] values = new Integer[n];
        java.util.HashMap<Integer, Integer> ref = new java.util.HashMap<>(n * 2);

        for (int i = 0; i < n; i++) {
            int k = i - 1000; // [-1000..999]
            Integer v = (rnd.nextInt(10) == 0) ? null : rnd.nextInt(); // иногда null
            keys[i] = k;
            values[i] = v;
            ref.put(k, v);
        }

        PerfectHashTable<Integer> table = PerfectHashTable.build(keys, values, seed);

        // свои ключи
        for (int k : keys) {
            assertTrue(table.containsKey(k));
            assertEquals(ref.get(k), table.get(k));
        }

        // чужие ключи
        for (int t = 0; t < 500; t++) {
            int k = 100_000 + t;
            assertFalse(table.containsKey(k));
            assertNull(table.get(k));
        }
    }

    @Test
    void randomized_sparseUniqueKeys_matchesHashMap() {
        long seed = 777L;
        java.util.SplittableRandom rnd = new java.util.SplittableRandom(seed);

        int n = 3000;
        int[] keys = new int[n];
        Integer[] values = new Integer[n];
        java.util.HashMap<Integer, Integer> ref = new java.util.HashMap<>(n * 2);
        java.util.HashSet<Integer> seen = new java.util.HashSet<>(n * 2);

        int i = 0;
        while (i < n) {
            int k = rnd.nextInt(); // разреженно
            if (!seen.add(k)) {
                continue;
            }
            Integer v = (rnd.nextInt(20) == 0) ? null : rnd.nextInt();
            keys[i] = k;
            values[i] = v;
            ref.put(k, v);
            i++;
        }

        PerfectHashTable<Integer> table = PerfectHashTable.build(keys, values, seed);

        for (int k : keys) {
            assertTrue(table.containsKey(k));
            assertEquals(ref.get(k), table.get(k));
        }

        // генерируем и пропускаем те, что случайно попали в set
        int checked = 0;
        while (checked < 1000) {
            int k = rnd.nextInt();
            if (seen.contains(k)) {
                continue;
            }
            assertFalse(table.containsKey(k));
            assertNull(table.get(k));
            checked++;
        }
    }
}
