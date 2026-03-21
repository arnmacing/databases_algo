package geosearch;

import java.util.ArrayList;
import java.util.List;

final class QuadTree<T> {
    private final int leafCapacity;
    private final int maxDepth;
    private final Node<T> root;

    QuadTree(
            double latMin,
            double latMax,
            double lonMin,
            double lonMax,
            int leafCapacity,
            int maxDepth) {
        this.leafCapacity = leafCapacity;
        this.maxDepth = maxDepth;
        this.root = Node.leaf(latMin, latMax, lonMin, lonMax);
    }

    void insert(GeoObject<T> object) {
        insertIntoTree(root, object, 0);
    }

    boolean remove(GeoObject<T> object) {
        return removeFromTree(root, object);
    }

    void collectInRange(
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude,
            List<GeoObject<T>> out) {
        collectInRange(root, minLatitude, maxLatitude, minLongitude, maxLongitude, out);
    }

    private void insertIntoTree(Node<T> node, GeoObject<T> object, int depth) {
        if (node.isLeaf()) {
            node.entries.add(object);
            if (node.entries.size() > leafCapacity && depth < maxDepth) {
                split(node, depth);
            }
            return;
        }

        Node<T> child = node.children[childIndex(node, object.latitude(), object.longitude())];
        insertIntoTree(child, object, depth + 1);
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

        List<GeoObject<T>> oldEntries = node.entries;
        node.entries = null;
        node.children = children;

        for (GeoObject<T> entry : oldEntries) {
            Node<T> child = node.children[childIndex(node, entry.latitude(), entry.longitude())];
            insertIntoTree(child, entry, depth + 1);
        }
    }

    private boolean removeFromTree(Node<T> node, GeoObject<T> object) {
        if (node.isLeaf()) {
            for (int i = 0; i < node.entries.size(); i++) {
                if (node.entries.get(i).id().equals(object.id())) {
                    node.entries.remove(i);
                    return true;
                }
            }
            return false;
        }

        Node<T> child = node.children[childIndex(node, object.latitude(), object.longitude())];
        return removeFromTree(child, object);
    }

    private void collectInRange(
            Node<T> node,
            double minLatitude,
            double maxLatitude,
            double minLongitude,
            double maxLongitude,
            List<GeoObject<T>> out) {

        if (!intersects(node, minLatitude, maxLatitude, minLongitude, maxLongitude)) {
            return;
        }

        if (node.isLeaf()) {
            for (GeoObject<T> entry : node.entries) {
                double latitude = entry.latitude();
                double longitude = entry.longitude();
                if (latitude >= minLatitude
                        && latitude <= maxLatitude
                        && longitude >= minLongitude
                        && longitude <= maxLongitude) {
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

        return node.latMax >= minLatitude
                && node.latMin <= maxLatitude
                && node.lonMax >= minLongitude
                && node.lonMin <= maxLongitude;
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

    private static final class Node<T> {
        private final double latMin;
        private final double latMax;
        private final double lonMin;
        private final double lonMax;
        private List<GeoObject<T>> entries;
        private Node<T>[] children;

        private Node(double latMin, double latMax, double lonMin, double lonMax, List<GeoObject<T>> entries) {
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
