package extendableHashing;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ExtendableHashTableTest {
    @Test
    void put_then_get_returns_value() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        assertEquals(10, ht.get(1));
    }

    @Test
    void get_missing_returns_null() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        assertNull(ht.get(404));
    }

    @Test
    void split_keepsAllKeys() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        ht.put(2, 20);
        ht.put(3, 30);

        assertEquals(10, ht.get(1));
        assertEquals(20, ht.get(2));
        assertEquals(30, ht.get(3));
    }

    @Test
    void put_SameKey_doesNotInsertNewRecord_keepsOldValue() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        ht.put(1, 99);
        assertEquals(10, ht.get(1));
    }

    @Test
    void remove_existing_removesAndGetReturnsNull() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        assertTrue(ht.remove(1));
        assertNull(ht.get(1));
    }

    @Test
    void remove_missing_returnsFalse() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        assertFalse(ht.remove(404));
    }

    @Test
    void remove_oneKey_doesNotAffectOthers() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        ht.put(2, 20);
        ht.put(3, 30);

        assertTrue(ht.remove(2));

        assertEquals(10, ht.get(1));
        assertNull(ht.get(2));
        assertEquals(30, ht.get(3));
    }

    @Test
    void merge_reducesUniqueBucketCount() {
        ExtendableHashTable ht = new ExtendableHashTable(2, 42L);

        List<Integer> insertedKeys = new ArrayList<>();

        for (int k = 0; k < 1000; k++) {
            ht.put(k, k);
            insertedKeys.add(k);

            if (ht.uniqueBucketCountForTest() > 1) {
                break;
            }
        }

        assertTrue(ht.uniqueBucketCountForTest() > 1, "бакетов всё еще 1");

        int before = ht.uniqueBucketCountForTest();

        for (int k : insertedKeys) {
            assertTrue(ht.remove(k), "ключ удалился");
        }

        int after = ht.uniqueBucketCountForTest();

        assertTrue(after < before, "уменьшение числа бакетов");
        assertEquals(1, after, "схлопнулось в 1 бакет");

    }
}
