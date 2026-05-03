package protocol;

public record RequestApi(short key, short version) {

    public static RequestApi of(int key, int version) {
        return new RequestApi((short) key, (short) version);
    }

    public static RequestApi of(short key, short version) {
        return new RequestApi(key, version);
    }
}
