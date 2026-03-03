package extendableHashing;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
    void put_sameKey_updatesValue() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.put(1, 10);
        ht.put(1, 99);
        assertEquals(99, ht.get(1));
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

    @Test
    void randomized_operations_matchHashMap() {
        long seed = 123456789L;
        Random rnd = new Random(seed);

        ExtendableHashTable ht = new ExtendableHashTable(2, seed);
        java.util.HashMap<Integer, Integer> ref = new java.util.HashMap<>();

        int ops = 50_000;

        for (int i = 0; i < ops; i++) {
            int key = rnd.nextInt(2000) - 1000; // отрицательные тоже
            int action = rnd.nextInt(4);

            switch (action) {
                case 0 -> { // put/upsert
                    int value = rnd.nextInt();
                    ht.put(key, value);
                    ref.put(key, value);
                }
                case 1 -> { // remove
                    boolean a = ht.remove(key);
                    boolean b = (ref.remove(key) != null);
                    assertEquals(b, a, "remove mismatch for key=" + key);
                }
                case 2 -> { // get
                    Integer a = ht.get(key);
                    Integer b = ref.get(key);
                    assertEquals(b, a, "get mismatch for key=" + key);
                }
                case 3 -> { // update
                    int value = rnd.nextInt();
                    ht.put(key, value);
                    ref.put(key, value);
                    assertEquals(ref.get(key), ht.get(key));
                }
            }
        }
    }
}
