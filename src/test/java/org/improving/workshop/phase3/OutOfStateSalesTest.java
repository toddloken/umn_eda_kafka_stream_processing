package org.improving.workshop.phase3;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OutOfStateSalesTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        OutOfStateSales.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                OutOfStateSales.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                OutOfStateSales.LINKED_HASH_MAP_JSON_SERDE.deserializer()
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