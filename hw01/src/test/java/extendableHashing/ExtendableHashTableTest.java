package extendableHashing;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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
    void split_keepsAllKeys() {
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
    void put_sameKey_updatesValue() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            ht.put(1, 99);
            assertEquals(99, ht.get(1));
        }
    }

    @Test
    void remove_existing_removesAndGetReturnsNull() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            ht.put(1, 10);
            assertTrue(ht.remove(1));
            assertNull(ht.get(1));
        }
    }

    @Test
    void remove_missing_returnsFalse() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2)) {
            assertFalse(ht.remove(404));
        }
    }

    @Test
    void remove_oneKey_doesNotAffectOthers() {
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
    void merge_reducesUniqueBucketCount() {
        try (ExtendableHashTable ht = new ExtendableHashTable(2, 42L)) {
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

    @Test
    void randomized_operations_matchHashMap() {
        long seed = 123456789L;
        Random rnd = new Random(seed);

        try (ExtendableHashTable ht = new ExtendableHashTable(2, seed)) {
            java.util.HashMap<Integer, Integer> ref = new java.util.HashMap<>();

            int ops = 50_000;

            for (int i = 0; i < ops; i++) {
                int key = rnd.nextInt(2000) - 1000; // отрицательные тоже
                int action = rnd.nextInt(4);

                switch (action) {
                    case 0 -> {
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

    @Test
    void reopen_readsPersistedData() throws IOException {
        Path file = Files.createTempFile("extendable-hash-test-", ".dat");
        try {
            try (ExtendableHashTable ht = new ExtendableHashTable(file, 2, 42L)) {
                ht.put(1, 10);
                ht.put(2, 20);
                ht.put(3, 30);
                ht.put(1, 99);
                assertTrue(ht.remove(2));
            }

            try (ExtendableHashTable reopened = ExtendableHashTable.open(file)) {
                assertEquals(99, reopened.get(1));
                assertNull(reopened.get(2));
                assertEquals(30, reopened.get(3));
                reopened.put(4, 40);
            }

            try (ExtendableHashTable reopenedAgain = ExtendableHashTable.open(file)) {
                assertEquals(40, reopenedAgain.get(4));
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void operations_after_close_throw() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        ht.close();
        assertThrows(IllegalStateException.class, () -> ht.put(1, 1));
        assertThrows(IllegalStateException.class, () -> ht.get(1));
        assertThrows(IllegalStateException.class, () -> ht.remove(1));
    }

    @Test
    void path_constructor_exposes_filePath_and_keeps_file_after_close() throws IOException {
        Path file = Files.createTempFile("extendable-hash-path-", ".dat");
        try {
            ExtendableHashTable ht = new ExtendableHashTable(file, 2);
            assertEquals(file, ht.filePath());

            ht.put(1, 10);
            ht.close();

            assertTrue(Files.exists(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void close_on_temp_table_deletes_file_and_is_idempotent() {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        Path file = ht.filePath();
        assertTrue(Files.exists(file));

        ht.close();
        assertFalse(Files.exists(file));

        assertDoesNotThrow(ht::close);
    }

    @Test
    void constructor_rejects_non_positive_bucket_capacity() {
        assertThrows(IllegalArgumentException.class, () -> new ExtendableHashTable(0));
        assertThrows(IllegalArgumentException.class, () -> new ExtendableHashTable(-1));
        assertThrows(IllegalArgumentException.class, () -> new ExtendableHashTable(Path.of("ignored.dat"), 0));
    }

    @Test
    void open_rejects_too_small_file() throws IOException {
        Path file = Files.createTempFile("extendable-hash-small-", ".dat");
        try {
            Files.write(file, new byte[16], StandardOpenOption.TRUNCATE_EXISTING);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("слишком маленький"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_magic() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteInt(file, 0, 0x12345678);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("magic"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_version() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteInt(file, 4, 999);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("версия"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_max_global_depth() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteInt(file, 32, 7);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("формат структуры"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_bucket_capacity_from_header() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteInt(file, 8, 0);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("bucketCapacity"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_global_depth_from_header() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteInt(file, 12, -1);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("globalDepth"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_rejects_invalid_next_free_offset_from_header() throws IOException {
        Path file = createPersistentTableFile();
        try {
            overwriteLong(file, 24, 1L);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ExtendableHashTable.open(file));
            assertTrue(ex.getMessage().contains("nextFreeOffset"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void open_non_existing_file_throws_unchecked_io_exception() throws IOException {
        Path dir = Files.createTempDirectory("extendable-hash-missing-");
        Path missingFile = dir.resolve("table.dat");
        try {
            UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> ExtendableHashTable.open(missingFile));
            assertTrue(ex.getMessage().contains("Не удалось открыть"));
        } finally {
            Files.deleteIfExists(dir);
        }
    }

    @Test
    void close_handles_null_mapped_buffer() throws Exception {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        Path file = ht.filePath();

        var bufferField = ExtendableHashTable.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        bufferField.set(ht, null);

        ht.close();
        assertFalse(Files.exists(file));
    }

    @Test
    void close_throws_when_delete_on_close_fails() throws Exception {
        ExtendableHashTable ht = new ExtendableHashTable(2);
        Path originalTableFile = ht.filePath();
        Path nonEmptyDir = Files.createTempDirectory("extendable-hash-dir-");
        Path marker = nonEmptyDir.resolve("keep.txt");
        Files.write(marker, new byte[]{1});

        try {
            var filePathField = ExtendableHashTable.class.getDeclaredField("filePath");
            filePathField.setAccessible(true);
            filePathField.set(ht, nonEmptyDir);

            UncheckedIOException ex = assertThrows(UncheckedIOException.class, ht::close);
            assertTrue(ex.getMessage().contains("Failed to close"));
        } finally {
            Files.deleteIfExists(originalTableFile);
            Files.deleteIfExists(marker);
            Files.deleteIfExists(nonEmptyDir);
        }
    }

    private static Path createPersistentTableFile() throws IOException {
        Path file = Files.createTempFile("extendable-hash-corrupt-", ".dat");
        try (ExtendableHashTable ht = new ExtendableHashTable(file, 2, 42L)) {
            ht.put(1, 10);
        }
        return file;
    }

    private static void overwriteInt(Path file, long offset, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        buffer.flip();

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.position(offset);
            channel.write(buffer);
        }
    }

    private static void overwriteLong(Path file, long offset, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        buffer.flip();

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.position(offset);
            channel.write(buffer);
        }
    }
}
