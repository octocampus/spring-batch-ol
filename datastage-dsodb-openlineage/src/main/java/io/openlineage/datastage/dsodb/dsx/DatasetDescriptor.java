package io.openlineage.datastage.dsodb.dsx;

public record DatasetDescriptor(String namespace, String name, Type type, String stageName) {

    public enum Type {
        INPUT, OUTPUT
    }
}
