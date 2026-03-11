package geosearch;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoSpatialIndexTest {
    private static final double EARTH_RADIUS_METERS = 6_371_008.8;
    private static final Comparator<GeoSearchResult<Integer>> RESULT_COMPARATOR = Comparator
            .comparingDouble(GeoSearchResult<Integer>::distanceMeters)
            .thenComparing(result -> result.object().id());

    @Test
    void coreFunctionality_shouldWork() {
        GeoSpatialIndex<String> index = new GeoSpatialIndex<>(200.0);
        index.put("A", 55.7558, 37.6173, "center");
        index.put("B", 55.7561, 37.6179, "near");
        index.put("C", 55.7600, 37.6200, "edge");
        index.put("D", 55.7800, 37.6500, "far");
        index.put(new GeoObject<>("origin", 10.0, 20.0, "point"));
        index.put("shop-42", 55.7558, 37.6173, "v1");
        index.put("shop-42", 55.7800, 37.6500, "v2");
        index.put("x", 59.9386, 30.3141, "spb");

        List<GeoSearchResult<String>> results = index.findNearby(55.7558, 37.6173, 700.0);
        assertEquals(List.of("A", "B", "C"),
                results.stream().map(result -> result.object().id()).toList());
        assertTrue(results.get(0).distanceMeters() <= results.get(1).distanceMeters());
        assertTrue(results.get(1).distanceMeters() <= results.get(2).distanceMeters());
        assertFalse(results.stream().anyMatch(result -> result.object().id().equals("D")));

        assertEquals("v2", index.getById("shop-42").orElseThrow().payload());
        assertTrue(index.getById("missing").isEmpty());
        assertTrue(index.findNearby(55.7558, 37.6173, 300.0)
                .stream().noneMatch(r -> r.object().id().equals("shop-42")));

        List<GeoSearchResult<String>> nearUpdated = index.findNearby(55.7800, 37.6500, 300.0);
        assertTrue(nearUpdated.stream().anyMatch(r -> r.object().id().equals("shop-42")));
        assertEquals("v2", nearUpdated.stream()
                .filter(r -> r.object().id().equals("shop-42"))
                .findFirst()
                .orElseThrow()
                .object()
                .payload());

        assertTrue(index.remove("x"));
        assertFalse(index.remove("x"));
        assertTrue(index.findNearby(59.9386, 30.3141, 500.0).isEmpty());

        List<GeoSearchResult<String>> exact = index.findNearby(10.0, 20.0, 0.0);
        assertEquals(1, exact.size());
        assertEquals("origin", exact.get(0).object().id());
        assertEquals(0.0, exact.get(0).distanceMeters());

        assertEquals(6, index.size());
    }

    @Test
    void geospatialSpecificCases_shouldWork() {
        GeoSpatialIndex<String> index = new GeoSpatialIndex<>(500.0);
        index.put("east-edge", 0.0, 179.9990, "east");
        index.put("west-edge", 0.0, -179.9990, "west");
        index.put("polar-east", 80.0, 0.0518, "near-80N");
        index.put("z-id", 15.0, 25.0, "z");
        index.put("a-id", 15.0, 25.0, "a");
        index.put("center", 0.0, 0.0, "center");

        List<GeoSearchResult<String>> leftWrap = index.findNearby(0.0, -179.9990, 400.0);
        assertEquals(List.of("west-edge", "east-edge"), leftWrap.stream().map(r -> r.object().id()).toList());

        List<GeoSearchResult<String>> rightWrap = index.findNearby(0.0, 179.9990, 400.0);
        assertEquals(List.of("east-edge", "west-edge"), rightWrap.stream().map(r -> r.object().id()).toList());

        List<GeoSearchResult<String>> highLatitude = index.findNearby(80.0, 0.0, 1_200.0);
        assertEquals(List.of("polar-east"), highLatitude.stream().map(r -> r.object().id()).toList());

        List<GeoSearchResult<String>> tieBreak = index.findNearby(15.0, 25.0, 1.0);
        assertEquals(List.of("a-id", "z-id"),
                tieBreak.stream().map(result -> result.object().id()).toList());

        List<GeoSearchResult<String>> global = index.findNearby(0.0, 0.0, 21_000_000.0);
        assertEquals(6, global.size());
    }

    @Test
    void quadtree_shouldSplitAndHandleRecursivePaths() {
        GeoSpatialIndex<Integer> index = new GeoSpatialIndex<>(150.0);
        for (int row = 0; row < 5; row++) {
            for (int col = 0; col < 8; col++) {
                String id = "p-" + row + "-" + col;
                double latitude = -80.0 + row * 40.0;
                double longitude = -170.0 + col * 50.0;
                index.put(id, latitude, longitude, row * 10 + col);
            }
        }

        assertEquals(40, index.size());

        List<GeoSearchResult<Integer>> aroundPoint = index.findNearby(40.0, 30.0, 5_000.0);
        assertEquals(1, aroundPoint.size());
        assertEquals("p-3-4", aroundPoint.get(0).object().id());

        assertTrue(index.remove("p-3-4"));
        assertFalse(index.remove("p-3-4"));
        assertTrue(index.findNearby(40.0, 30.0, 5_000.0).isEmpty());
    }

    @Test
    void constructors_shouldHandleLargeCellSizes() {
        GeoSpatialIndex<String> lowDepth = new GeoSpatialIndex<>(30_000_000.0);
        lowDepth.put("x", 0.0, 0.0, "x");
        assertTrue(lowDepth.getById("x").isPresent());

        GeoSpatialIndex<String> singleCell = new GeoSpatialIndex<>(50_000_000.0);
        singleCell.put("y", 0.0, 0.0, "y");
        assertEquals(1, singleCell.findNearby(0.0, 0.0, 1.0).size());
    }

    @Test
    void validations_shouldBeRejected() {
        assertDoesNotThrow(() -> new GeoObject<>("id", 0.0, 0.0, "ok"));
        assertDoesNotThrow(() -> new GeoSearchResult<>(new GeoObject<>("id", 0.0, 0.0, "ok"), 0.0));

        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>(null, 0.0, 0.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>(" ", 0.0, 0.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", -91.0, 0.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", 91.0, 0.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", 0.0, -181.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", 0.0, 181.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", Double.NaN, 0.0, "x"));
        assertThrows(IllegalArgumentException.class, () -> new GeoObject<>("id", 0.0, Double.NaN, "x"));

        GeoObject<String> obj = new GeoObject<>("id", 0.0, 0.0, "x");
        assertThrows(NullPointerException.class, () -> new GeoSearchResult<>(null, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new GeoSearchResult<>(obj, -0.1));
        assertThrows(IllegalArgumentException.class, () -> new GeoSearchResult<>(obj, Double.NaN));

        assertThrows(IllegalArgumentException.class, () -> new GeoSpatialIndex<>(0.0));
        assertThrows(IllegalArgumentException.class, () -> new GeoSpatialIndex<>(Double.NaN));

        GeoSpatialIndex<String> index = new GeoSpatialIndex<>();
        assertThrows(IllegalArgumentException.class, () -> index.put(null, 0.0, 0.0, "null-id"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", -91.0, 0.0, "bad-lat-low"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", 91.0, 0.0, "bad-lat"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", 0.0, -181.0, "bad-lon-low"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", 0.0, 181.0, "bad-lon"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", Double.NaN, 0.0, "nan-lat"));
        assertThrows(IllegalArgumentException.class, () -> index.put("a", 0.0, Double.POSITIVE_INFINITY, "inf-lon"));
        assertThrows(IllegalArgumentException.class, () -> index.put(" ", 0.0, 0.0, "blank-id"));
        assertThrows(IllegalArgumentException.class, () -> index.put((GeoObject<String>) null));

        assertThrows(IllegalArgumentException.class, () -> index.findNearby(-91.0, 0.0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(91.0, 0.0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(0.0, -181.0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(0.0, 181.0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(Double.NaN, 0.0, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(0.0, Double.NaN, 1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(0.0, 0.0, -1.0));
        assertThrows(IllegalArgumentException.class, () -> index.findNearby(0.0, 0.0, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> index.getById(""));
        assertThrows(IllegalArgumentException.class, () -> index.remove(null));
    }

    @Test
    void randomizedDataset_shouldMatchNaiveReference() {
        SplittableRandom rnd = new SplittableRandom(20260311L);
        GeoSpatialIndex<Integer> index = new GeoSpatialIndex<>(140.0);
        Map<String, GeoObject<Integer>> reference = new HashMap<>();

        int idPool = 320;
        for (int step = 0; step < 2_800; step++) {
            int op = rnd.nextInt(100);

            if (op < 50) {
                String id = "key-" + rnd.nextInt(idPool);
                GeoObject<Integer> object = randomObject(id, step, rnd);
                reference.put(id, object);
                index.put(object);
            } else if (op < 70) {
                String id = "key-" + rnd.nextInt(idPool + 40);
                boolean actualRemoved = index.remove(id);
                boolean expectedRemoved = reference.remove(id) != null;
                assertEquals(expectedRemoved, actualRemoved, "remove mismatch for id=" + id);
            } else if (op < 90) {
                double latitude = randomLatitude(rnd);
                double longitude = randomLongitude(rnd);
                double radiusMeters = randomRadius(rnd);

                List<GeoSearchResult<Integer>> actual = index.findNearby(latitude, longitude, radiusMeters);
                List<GeoSearchResult<Integer>> expected = naiveSearch(reference, latitude, longitude, radiusMeters);
                assertSameResults(expected, actual);
            } else {
                String id = "key-" + rnd.nextInt(idPool + 40);
                Optional<GeoObject<Integer>> byId = index.getById(id);
                assertEquals(reference.get(id), byId.orElse(null), "getById mismatch for id=" + id);
            }

            if (step % 100 == 0) {
                assertEquals(reference.size(), index.size(), "size mismatch at step=" + step);
            }
        }
    }

    private static GeoObject<Integer> randomObject(String id, int payload, SplittableRandom rnd) {
        return new GeoObject<>(id, randomLatitude(rnd), randomLongitude(rnd), payload);
    }

    private static double randomLatitude(SplittableRandom rnd) {
        int mode = rnd.nextInt(12);
        if (mode == 0) {
            return -90.0 + rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 1) {
            return 90.0 - rnd.nextDouble(0.0, 0.2);
        }
        return rnd.nextDouble(-90.0, 90.0);
    }

    private static double randomLongitude(SplittableRandom rnd) {
        int mode = rnd.nextInt(14);
        if (mode == 0) {
            return -180.0 + rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 1) {
            return 180.0 - rnd.nextDouble(0.0, 0.2);
        }
        if (mode == 2) {
            return rnd.nextDouble(-180.0, -179.7);
        }
        if (mode == 3) {
            return rnd.nextDouble(179.7, 180.0);
        }
        return rnd.nextDouble(-180.0, 180.0);
    }

    private static double randomRadius(SplittableRandom rnd) {
        return switch (rnd.nextInt(10)) {
            case 0 -> 0.0;
            case 1 -> rnd.nextDouble(0.1, 50.0);
            case 2 -> rnd.nextDouble(50.0, 2_000.0);
            case 3 -> rnd.nextDouble(2_000.0, 20_000.0);
            case 4 -> rnd.nextDouble(20_000.0, 200_000.0);
            case 5 -> rnd.nextDouble(200_000.0, 2_000_000.0);
            case 6 -> rnd.nextDouble(2_000_000.0, 10_000_000.0);
            case 7 -> rnd.nextDouble(10_000_000.0, 18_000_000.0);
            case 8 -> 21_000_000.0;
            default -> rnd.nextDouble(0.0, 21_000_000.0);
        };
    }

    private static List<GeoSearchResult<Integer>> naiveSearch(
            Map<String, GeoObject<Integer>> reference,
            double queryLatitude,
            double queryLongitude,
            double radiusMeters) {

        List<GeoSearchResult<Integer>> results = new ArrayList<>();
        for (GeoObject<Integer> object : reference.values()) {
            double distance = haversineMeters(
                    queryLatitude, queryLongitude, object.latitude(), object.longitude());
            if (distance <= radiusMeters) {
                results.add(new GeoSearchResult<>(object, distance));
            }
        }
        results.sort(RESULT_COMPARATOR);
        return results;
    }

    private static void assertSameResults(
            List<GeoSearchResult<Integer>> expected,
            List<GeoSearchResult<Integer>> actual) {

        assertEquals(expected.size(), actual.size(), "different result size");
        for (int i = 0; i < expected.size(); i++) {
            GeoSearchResult<Integer> exp = expected.get(i);
            GeoSearchResult<Integer> act = actual.get(i);
            assertEquals(exp.object().id(), act.object().id(), "different id at index=" + i);
            assertEquals(exp.object().payload(), act.object().payload(), "different payload at index=" + i);
            assertEquals(exp.distanceMeters(), act.distanceMeters(), 1.0e-6, "different distance at index=" + i);
        }
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double dLat = lat2Rad - lat1Rad;
        double dLon = Math.toRadians(lon2 - lon1);

        double sinHalfLat = Math.sin(dLat / 2.0);
        double sinHalfLon = Math.sin(dLon / 2.0);

        double a = sinHalfLat * sinHalfLat
                + Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinHalfLon * sinHalfLon;
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        return EARTH_RADIUS_METERS * c;
    }
}
