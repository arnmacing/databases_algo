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
        Integer[] values = {10, 20, 30};

        PerfectHashTable<Integer> table = PerfectHashTable.build(keys, values, 42L);

        for (int key : keys) {
            assertTrue(table.containsKey(key));
            assertNotNull(table.get(key));
        }

        assertFalse(table.containsKey(4));
        assertNull(table.get(4));
    }
}
