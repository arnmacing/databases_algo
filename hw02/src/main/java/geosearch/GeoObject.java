package geosearch;

public record GeoObject<T>(String id, double latitude, double longitude, T payload) {

    public GeoObject {
        validateId(id);
        validateLatitude(latitude);
        validateLongitude(longitude);
    }

    private static void validateId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be null or blank");
        }
    }

    private static void validateLatitude(double latitude) {
        if (!Double.isFinite(latitude) || latitude < -90.0 || latitude > 90.0) {
            throw new IllegalArgumentException("latitude must be in [-90, 90] and finite");
        }
    }

    private static void validateLongitude(double longitude) {
        if (!Double.isFinite(longitude) || longitude < -180.0 || longitude > 180.0) {
            throw new IllegalArgumentException("longitude must be in [-180, 180] and finite");
        }
    }
}
