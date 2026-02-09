package io.openlineage.batch.demo;

import org.springframework.batch.item.ItemProcessor;

public class PersonEnrichProcessor implements ItemProcessor<Person, EnrichedPerson> {

    @Override
    public EnrichedPerson process(Person person) {
        String fullName = person.firstName() + " " + person.lastName();
        String category = person.age() < 30 ? "junior" : "senior";
        return new EnrichedPerson(
                person.firstName(), person.lastName(), person.email(), person.age(),
                fullName, category);
    }
}
