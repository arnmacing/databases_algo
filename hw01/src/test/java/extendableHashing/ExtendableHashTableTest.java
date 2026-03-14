package extendableHashing;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class ExtendableHashTableTest {
    @Test
    void put_then_get_returns_value() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            assertEquals(10, ht.get(1));
        }
    }

    @Test
    void get_missing_returns_null() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            assertNull(ht.get(404));
        }
    }

    @Test
    void split_keeps_all_keys() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            ht.put(2, 20);
            ht.put(3, 30);

            assertEquals(10, ht.get(1));
            assertEquals(20, ht.get(2));
            assertEquals(30, ht.get(3));
        }
    }

    @Test
    void put_same_key_updates_value() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            ht.put(1, 99);
            assertEquals(99, ht.get(1));
        }
    }

    @Test
    void remove_existing_removes_key() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            assertTrue(ht.remove(1));
            assertNull(ht.get(1));
        }
    }

    @Test
    void remove_missing_returns_false() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            assertFalse(ht.remove(404));
        }
    }

    @Test
    void remove_one_key_does_not_affect_others() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            ht.put(2, 20);
            ht.put(3, 30);

            assertTrue(ht.remove(2));
            assertEquals(10, ht.get(1));
            assertNull(ht.get(2));
            assertEquals(30, ht.get(3));
        }
    }

    @Test
    void randomized_operations_match_hash_map() {
        long seed = 123456789L;
        Random rnd = new Random(seed);

        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            HashMap<Integer, Integer> ref = new HashMap<>();

            int ops = 50_000;

            for (int i = 0; i < ops; i++) {
                int key = rnd.nextInt(2000) - 1000;
                int action = rnd.nextInt(4);

                switch (action) {
                    case 0 -> {
                        int value = rnd.nextInt();
                        ht.put(key, value);
                        ref.put(key, value);
                    }
                    case 1 -> {
                        boolean a = ht.remove(key);
                        boolean b = (ref.remove(key) != null);
                        assertEquals(b, a);
                    }
                    case 2 -> assertEquals(ref.get(key), ht.get(key));
                    case 3 -> {
                        int value = rnd.nextInt();
                        ht.put(key, value);
                        ref.put(key, value);
                        assertEquals(ref.get(key), ht.get(key));
                    }
                }
            }
        }
    }

    @Test
    void close_is_idempotent() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.close();
        assertDoesNotThrow(ht::close);
    }
}
