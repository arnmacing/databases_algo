package geosearch;

public record GeoSearchResult<T>(GeoObject<T> object, double distanceMeters) {
}
