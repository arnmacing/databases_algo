package extendableHashing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class BucketTest {

    @Test
    void containsKey_shouldWork() {
        Bucket b = new Bucket(0, 2);

        assertFalse(b.containsKey(10));
        assertEquals(PutResult.INSERTED, b.put(10, 100));
        assertTrue(b.containsKey(10));
        assertEquals(100, b.get(10));
        assertFalse(b.containsKey(11));
    }
}