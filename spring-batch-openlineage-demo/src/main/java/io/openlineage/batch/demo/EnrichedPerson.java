package io.openlineage.batch.demo;

public record EnrichedPerson(String firstName, String lastName, String email, int age,
                              String fullName, String category) {
}
