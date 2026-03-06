package io.openlineage.datastage.flow;

public record DatasetDescriptor(String namespace, String name, Type type) {

    public enum Type {
        INPUT, OUTPUT
    }
}
