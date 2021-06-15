package io.confluent.examples.clients.basicavro;

import com.fasterxml.jackson.annotation.JsonProperty;

public class User {
    @JsonProperty
    public String firstName;

    @JsonProperty
    public String lastName;

    @JsonProperty
    public short age;

    public User() {
    }

    public User(String firstname, String lastName, short age) {
        this.firstName = firstname;
        this.lastName = lastName;
        this.age = age;
    }

}
