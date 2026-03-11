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

    private final int leafCapacity;
    private final int maxDepth;
    private final Node<T> root;
    private final Map<String, IndexEntry<T>> byId = new HashMap<>();

    public GeoSpatialIndex() {
        this(DEFAULT_CELL_SIZE_METERS);
    }

    public GeoSpatialIndex(double cellSizeMeters) {
        validateCellSize(cellSizeMeters);
        this.leafCapacity = DEFAULT_LEAF_CAPACITY;
        this.maxDepth = computeMaxDepth(cellSizeMeters);
        this.root = Node.leaf(MIN_LATITUDE, MAX_LATITUDE, MIN_LONGITUDE, MAX_LONGITUDE);
    }

    public void put(String id, double latitude, double longitude, T payload) {
        GeoObject<T> object = new GeoObject<>(id, latitude, longitude, payload);

        IndexEntry<T> oldEntry = byId.remove(id);
        if (oldEntry != null) {
            removeFromTree(root, oldEntry);
        }

        IndexEntry<T> newEntry = new IndexEntry<>(object);
        insertIntoTree(root, newEntry, 0);
        byId.put(id, newEntry);
    }

    public void put(GeoObject<T> object) {
        if (object == null) {
            throw new IllegalArgumentException("object must not be null");
        }
        put(object.id(), object.latitude(), object.longitude(), object.payload());
    }

    public boolean remove(String id) {
        validateId(id);
        IndexEntry<T> entry = byId.remove(id);
        if (entry == null) {
            return false;
        }
        return removeFromTree(root, entry);
    }

    public Optional<GeoObject<T>> getById(String id) {
        validateId(id);
        IndexEntry<T> entry = byId.get(id);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.of(entry.object);
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
        List<IndexEntry<T>> candidates = new ArrayList<>();
        for (LonWindow window : longitudeWindows) {
            collectInRange(root, minLatitude, maxLatitude, window.minLongitude, window.maxLongitude, candidates);
        }

        List<GeoSearchResult<T>> results = new ArrayList<>();
        Set<String> seenIds = new HashSet<>();
        for (IndexEntry<T> candidate : candidates) {
            String id = candidate.object.id();
            if (!seenIds.add(id)) {
                continue;
            }
            GeoObject<T> object = candidate.object;
            double distanceMeters = haversineMeters(latitude, longitude, object.latitude(), object.longitude());
            if (distanceMeters <= radiusMeters) {
                results.add(new GeoSearchResult<>(object, distanceMeters));
            }
        }

        results.sort(Comparator
                .comparingDouble(GeoSearchResult<T>::distanceMeters)
                .thenComparing(result -> result.object().id()));

        return results;
    }

    private void insertIntoTree(Node<T> node, IndexEntry<T> entry, int depth) {
        if (node.isLeaf()) {
            node.entries.add(entry);
            if (node.entries.size() > leafCapacity && depth < maxDepth) {
                split(node, depth);
            }
            return;
        }

        Node<T> child = node.children[childIndex(node, entry.object.latitude(), entry.object.longitude())];
        insertIntoTree(child, entry, depth + 1);
    }

    private void split(Node<T> node, int depth) {
        double midLat = midpoint(node.latMin, node.latMax);
        double midLon = midpoint(node.lonMin, node.lonMax);

        @SuppressWarnings("unchecked")
        Node<T>[] children = (Node<T>[]) new Node<?>[4];
        children[0] = Node.leaf(node.latMin, midLat, node.lonMin, midLon); // SW
        children[1] = Node.leaf(node.latMin, midLat, midLon, node.lonMax); // SE
        children[2] = Node.leaf(midLat, node.latMax, node.lonMin, midLon); // NW
        children[3] = Node.leaf(midLat, node.latMax, midLon, node.lonMax); // NE

        List<IndexEntry<T>> oldEntries = node.entries;
        node.entries = null;
        node.children = children;

        for (IndexEntry<T> entry : oldEntries) {
            Node<T> child = node.children[childIndex(node, entry.object.latitude(), entry.object.longitude())];
            insertIntoTree(child, entry, depth + 1);
        }
    }

    private boolean removeFromTree(Node<T> node, IndexEntry<T> entry) {
        if (node.isLeaf()) {
            for (int i = 0; i < node.entries.size(); i++) {
                if (node.entries.get(i).object.id().equals(entry.object.id())) {
                    node.entries.remove(i);
                    return true;
                }
            }
            return false;
        }

        Node<T> child = node.children[childIndex(node, entry.object.latitude(), entry.object.longitude())];
        return removeFromTree(child, entry);
    }

    private void collectInRange(
            Node<T> node,
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude,
            List<IndexEntry<T>> out) {

        if (!intersects(node, minLatitude, maxLatitude, minLongitude, maxLongitude)) {
            return;
        }

        if (node.isLeaf()) {
            for (IndexEntry<T> entry : node.entries) {
                double latitude = entry.object.latitude();
                double longitude = entry.object.longitude();
                if (latitude >= minLatitude && latitude <= maxLatitude
                        && longitude >= minLongitude && longitude <= maxLongitude) {
                    out.add(entry);
                }
            }
            return;
        }

        for (Node<T> child : node.children) {
            collectInRange(child, minLatitude, maxLatitude, minLongitude, maxLongitude, out);
        }
    }

    private static boolean intersects(
            Node<?> node,
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude) {

        return node.latMax >= minLatitude && node.latMin <= maxLatitude
                && node.lonMax >= minLongitude && node.lonMin <= maxLongitude;
    }

    private static int childIndex(Node<?> node, double latitude, double longitude) {
        double midLat = midpoint(node.latMin, node.latMax);
        double midLon = midpoint(node.lonMin, node.lonMax);
        int row = latitude >= midLat ? 1 : 0;
        int col = longitude >= midLon ? 1 : 0;
        return row * 2 + col;
    }

    private static double midpoint(double left, double right) {
        return left + (right - left) * 0.5;
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

    private static final class IndexEntry<T> {
        private final GeoObject<T> object;

        private IndexEntry(GeoObject<T> object) {
            this.object = object;
        }
    }

    private static final class Node<T> {
        private final double latMin;
        private final double latMax;
        private final double lonMin;
        private final double lonMax;
        private List<IndexEntry<T>> entries;
        private Node<T>[] children;

        private Node(double latMin, double latMax, double lonMin, double lonMax, List<IndexEntry<T>> entries) {
            this.latMin = latMin;
            this.latMax = latMax;
            this.lonMin = lonMin;
            this.lonMax = lonMax;
            this.entries = entries;
        }

        private static <T> Node<T> leaf(double latMin, double latMax, double lonMin, double lonMax) {
            return new Node<>(latMin, latMax, lonMin, lonMax, new ArrayList<>());
        }

        private boolean isLeaf() {
            return children == null;
        }
    }
}
