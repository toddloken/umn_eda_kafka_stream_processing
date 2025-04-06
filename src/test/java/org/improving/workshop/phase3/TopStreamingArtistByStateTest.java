package org.improving.workshop.phase3;

import lombok.extern.slf4j.Slf4j;
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
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class TopStreamingArtistByStateTest {
    private TopologyTestDriver driver;

    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;

    private TestOutputTopic<String, Artist> artistKTable;
    private TestOutputTopic<String, Customer> customerArtistKTable;
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

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        artistKTable = driver.createOutputTopic(
                TopSellingGenreByVenue.ARTIST_KTABLE,
                Serdes.String().deserializer(),
                Streams.SERDE_ARTIST_JSON.deserializer()
        );

//        customerArtistKTable = driver.createOutputTopic(
//                TopSellingGenreByVenue.CUSTOMER_ARE,
//                Serdes.String().deserializer(),
//                Streams.SERDE_ARTIST_JSON.deserializer()
//        );


        outputTopic = driver.createOutputTopic(
                TopStreamingArtistByState.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    public void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("Phase3 Top Streaming Artist in Each State")
    public void customerTopStreamedArtists() {
        // ARRANGE
        // Create multiple customer streams for the initial test scenario
        String customerId = "1";
        String artistId2 = "2";
        String artistId3 = "3";
        String artistId4 = "4";
        String artistId5 = "5";

        // ACT - First batch of stream events
        // Customer 1 listens to artist 2 twice
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId2));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId2));
        // Customer 1 listens to artist 3 once
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId3));
        // Customer 1 listens to artist 4 three times
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId4));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId4));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId4));

        // ASSERT - Verify initial top artists state
        var outputRecords = outputTopic.readRecordsToList();
        // Verify the expected number of records were received
        assertEquals(6, outputRecords.size(), "Should have received 6 output records");

        // Verify the last record holds the expected top 3 artists (artist 4, 2, 3 in that order)
        LinkedHashMap<String, Long> expectedInitialTop3 = java.util.stream.Stream.of(
                Map.entry(artistId4, 3L),
                Map.entry(artistId2, 2L),
                Map.entry(artistId3, 1L)
        ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll);

        assertEquals(expectedInitialTop3, outputRecords.getLast().value(),
                "Initial top 3 artists should be 4 (3 plays), 2 (2 plays), 3 (1 play)");

        // ACT - Second batch: Customer listens to artist 5 twice
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId5));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId5));

        // ASSERT - Verify artist 5 replaced artist 3 in the top 3
        LinkedHashMap<String, Long> expectedSecondTop3 = java.util.stream.Stream.of(
                Map.entry(artistId4, 3L),
                Map.entry(artistId2, 2L),
                Map.entry(artistId5, 2L)
        ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll);

        assertEquals(expectedSecondTop3, outputTopic.readRecordsToList().getLast().value(),
                "Updated top 3 should have artist 5 (2 plays) instead of artist 3 (1 play)");

        // ACT - Third batch: Customer listens to artist 3 two more times
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId3));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customerId, artistId3));

        // ASSERT - Verify artist 3 is back in the top 3, replacing artist 5
        LinkedHashMap<String, Long> expectedFinalTop3 = java.util.stream.Stream.of(
                Map.entry(artistId4, 3L),
                Map.entry(artistId3, 3L),
                Map.entry(artistId2, 2L)
        ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll);

        assertEquals(expectedFinalTop3, outputTopic.readRecordsToList().getLast().value(),
                "Final top 3 should have artist 3 (now 3 plays) back in, replacing artist 5 (2 plays)");
    }
}