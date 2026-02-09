package io.openlineage.batch.extractor;

public record DatasetInfo(String namespace, String name, Type type) {

    public enum Type {
        INPUT, OUTPUT
    }

    public static DatasetInfo unknown(Type type) {
        return new DatasetInfo("unknown", "UNKNOWN", type);
    }

    public boolean isUnknown() {
        return "UNKNOWN".equals(name);
    }
}
