package org.improving.workshop.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.customer.address.Address;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TopStreamingArtistByStateTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        TopStreamingArtistByState.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        );

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        );

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                TopStreamingArtistByState.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    public void cleanup() { driver.close(); }

    @Test
    @DisplayName("top young adult artists by state")
    public void insertTestHere() {
        // ARRANGE


        // ACT


        // ASSERT

    }
}