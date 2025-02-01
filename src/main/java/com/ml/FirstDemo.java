package com.ml;

import com.ml.entity.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * when run code need ad VM parameter:
 * --add-opens java.base/java.lang=ALL-UNNAMED
 */
public class FirstDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2),
                new Person("Tom",12),
                new Person("Jack",16),
                new Person("Lily",6));

        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();

//        DataStream<Person> children=flintstones.filter(new FilterFunction<Person>() {
//            @Override
//            public boolean filter(Person person) throws Exception {
//                return person.age<18;
//            }
//        });
//
//        children.print("Children are: ");

        env.execute();
    }
}
