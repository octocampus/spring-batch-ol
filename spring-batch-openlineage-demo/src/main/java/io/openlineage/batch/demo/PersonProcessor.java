package io.openlineage.batch.demo;

import org.springframework.batch.item.ItemProcessor;

public class PersonProcessor implements ItemProcessor<Person, Person> {

    @Override
    public Person process(Person person) {
        return person.withUpperCaseNames();
    }
}
