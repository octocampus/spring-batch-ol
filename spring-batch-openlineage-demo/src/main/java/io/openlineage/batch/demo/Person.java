package io.openlineage.batch.demo;

public record Person(String firstName, String lastName, String email, int age) {

    public Person withUpperCaseNames() {
        return new Person(
                firstName != null ? firstName.toUpperCase() : null,
                lastName != null ? lastName.toUpperCase() : null,
                email,
                age);
    }
}
