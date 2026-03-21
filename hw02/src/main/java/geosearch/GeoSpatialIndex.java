package geosearch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class GeoSpatialIndex<T> {
    private static final double EARTH_RADIUS_METERS = 6_371_008.8;
    private static final double WORLD_WIDTH_METERS = 2.0 * Math.PI * EARTH_RADIUS_METERS;
    private static final double MIN_LATITUDE = -90.0;
    private static final double MAX_LATITUDE = 90.0;
    private static final double MIN_LONGITUDE = -180.0;
    private static final double MAX_LONGITUDE = 180.0;
    private static final int DEFAULT_LEAF_CAPACITY = 16;
    private static final int MIN_TREE_DEPTH = 2;
    private static final int MAX_TREE_DEPTH = 24;

    public static final double DEFAULT_CELL_SIZE_METERS = 500.0;

    private final QuadTree<T> tree;
    private final Map<String, GeoObject<T>> byId = new HashMap<>();

    public GeoSpatialIndex() {
        this(DEFAULT_CELL_SIZE_METERS);
    }

    public GeoSpatialIndex(double cellSizeMeters) {
        validateCellSize(cellSizeMeters);
        int maxDepth = computeMaxDepth(cellSizeMeters);
        this.tree = new QuadTree<>(
                MIN_LATITUDE,
                MAX_LATITUDE,
                MIN_LONGITUDE,
                MAX_LONGITUDE,
                DEFAULT_LEAF_CAPACITY,
                maxDepth);
    }

    public void put(String id, double latitude, double longitude, T payload) {
        put(new GeoObject<>(id, latitude, longitude, payload));
    }

    public void put(GeoObject<T> object) {
        GeoObject<T> oldObject = byId.put(object.id(), object);
        if (oldObject != null) {
            tree.remove(oldObject);
        }
        tree.insert(object);
    }

    public boolean remove(String id) {
        validateId(id);
        GeoObject<T> object = byId.remove(id);
        if (object == null) {
            return false;
        }
        return tree.remove(object);
    }

    public Optional<GeoObject<T>> getById(String id) {
        validateId(id);
        return Optional.ofNullable(byId.get(id));
    }

    public int size() {
        return byId.size();
    }

    public List<GeoSearchResult<T>> findNearby(double latitude, double longitude, double radiusMeters) {
        validateLatitude(latitude);
        validateLongitude(longitude);
        validateRadius(radiusMeters);

        double angularRadiusDegrees = Math.toDegrees(radiusMeters / EARTH_RADIUS_METERS);
        double minLatitude = Math.max(MIN_LATITUDE, latitude - angularRadiusDegrees);
        double maxLatitude = Math.min(MAX_LATITUDE, latitude + angularRadiusDegrees);

        double maxAbsLatitude = Math.max(Math.abs(minLatitude), Math.abs(maxLatitude));
        double cosAtMaxAbsLatitude = Math.cos(Math.toRadians(maxAbsLatitude));
        double minCos = Math.max(cosAtMaxAbsLatitude, 1.0e-6);

        double longitudeDelta;
        if (radiusMeters == 0.0) {
            longitudeDelta = 0.0;
        } else {
            longitudeDelta = Math.toDegrees(radiusMeters / (EARTH_RADIUS_METERS * minCos));
            longitudeDelta = Math.min(MAX_LONGITUDE, longitudeDelta);
        }

        List<LonWindow> longitudeWindows = buildLongitudeWindows(longitude, longitudeDelta);
        List<GeoObject<T>> candidates = new ArrayList<>();
        for (LonWindow window : longitudeWindows) {
            tree.collectInRange(
                    minLatitude,
                    maxLatitude,
                    window.minLongitude,
                    window.maxLongitude,
                    candidates);
        }

        List<GeoSearchResult<T>> results = new ArrayList<>();
        Set<String> seenIds = new HashSet<>();
        for (GeoObject<T> candidate : candidates) {
            if (!seenIds.add(candidate.id())) {
                continue;
            }
            double distanceMeters = haversineMeters(
                    latitude,
                    longitude,
                    candidate.latitude(),
                    candidate.longitude());
            if (distanceMeters <= radiusMeters) {
                results.add(new GeoSearchResult<>(candidate, distanceMeters));
            }
        }

        results.sort(Comparator
                .comparingDouble(GeoSearchResult<T>::distanceMeters)
                .thenComparing(result -> result.object().id()));

        return results;
    }

    private static int computeMaxDepth(double cellSizeMeters) {
        double cellsPerWorld = WORLD_WIDTH_METERS / cellSizeMeters;
        if (cellsPerWorld <= 1.0) {
            return MIN_TREE_DEPTH;
        }
        int depth = (int) Math.ceil(Math.log(cellsPerWorld) / Math.log(2.0));
        if (depth < MIN_TREE_DEPTH) {
            return MIN_TREE_DEPTH;
        }
        return Math.min(depth, MAX_TREE_DEPTH);
    }

    private static List<LonWindow> buildLongitudeWindows(double longitude, double delta) {
        if (delta >= MAX_LONGITUDE) {
            return List.of(new LonWindow(MIN_LONGITUDE, MAX_LONGITUDE));
        }

        double left = longitude - delta;
        double right = longitude + delta;
        if (left < MIN_LONGITUDE) {
            return List.of(
                    new LonWindow(left + 360.0, MAX_LONGITUDE),
                    new LonWindow(MIN_LONGITUDE, right));
        }
        if (right > MAX_LONGITUDE) {
            return List.of(
                    new LonWindow(left, MAX_LONGITUDE),
                    new LonWindow(MIN_LONGITUDE, right - 360.0));
        }
        return List.of(new LonWindow(left, right));
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

    private static void validateId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be null or blank");
        }
    }

    private static void validateLatitude(double latitude) {
        if (!Double.isFinite(latitude) || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new IllegalArgumentException("latitude must be in [-90, 90] and finite");
        }
    }

    private static void validateLongitude(double longitude) {
        if (!Double.isFinite(longitude) || longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new IllegalArgumentException("longitude must be in [-180, 180] and finite");
        }
    }

    private static void validateRadius(double radiusMeters) {
        if (!Double.isFinite(radiusMeters) || radiusMeters < 0.0) {
            throw new IllegalArgumentException("radiusMeters must be finite and >= 0");
        }
    }

    private static void validateCellSize(double cellSizeMeters) {
        if (!Double.isFinite(cellSizeMeters) || cellSizeMeters <= 0.0) {
            throw new IllegalArgumentException("cellSizeMeters must be finite and > 0");
        }
    }

    private record LonWindow(double minLongitude, double maxLongitude) {
    }
}
