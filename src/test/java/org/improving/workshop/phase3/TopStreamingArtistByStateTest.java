package org.improving.workshop.phase3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.artist.ArtistFaker;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.stream.StreamFaker;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.improving.workshop.Streams.SERDE_STREAM_JSON;
import static org.improving.workshop.utils.DataFaker.ARTISTS;
import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.improving.workshop.utils.DataFaker.CUSTOMERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//=====================================================================================

@Slf4j
public class TopStreamingArtistByStateTest {

    private TopologyTestDriver testDriver;
    private StreamsBuilder streamsBuilder;
    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> enrichedArtistCustomerTopic;
    private TestOutputTopic<String, TopStreamingArtistByState.EnrichedStream> outputTopic;
    private KeyValueStore<String, Stream> streamStore;
    private KeyValueStore<String, Artist> artistStore;
    private StreamFaker streamFaker;
    private ArtistFaker artistFaker;

    //=====================================================================================
    // Before Each
    //=====================================================================================
    @BeforeEach
    public void setup() {
        // Create the test topology
        streamsBuilder = new StreamsBuilder();
        TopStreamingArtistByState.configureTopology(streamsBuilder);

        // Configure the test driver
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "test-kafka-streams-" + UUID.randomUUID());
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        // Create test topics
        streamInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.STREAM_INPUT_TOPIC,
                Serdes.String().serializer(),
                SERDE_STREAM_JSON.serializer()
        );

        artistInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.ARTIST_INPUT_TOPIC,
                Serdes.String().serializer(),
                TopStreamingArtistByState.ARTISTS_JSON_SERDE.serializer()
        );

        customerInputTopic = testDriver.createInputTopic(
                TopStreamingArtistByState.CUSTOMER_INPUT_TOPIC,
                Serdes.String().serializer(),
                TopStreamingArtistByState.SERDE_CUSTOMER_JSON.serializer()
        );

        enrichedArtistCustomerTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.ENRICHED_ARTIST_CUSTOMER_STREAM_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ENRICHED_ARTIST_CUSTOMER_STREAM_JSON_SERDE.deserializer()
        );

        outputTopic = testDriver.createOutputTopic(
                TopStreamingArtistByState.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.ENRICHED_STREAM_JSON_SERDE.deserializer()
        );

        // Get access to the state stores
        streamStore = testDriver.getKeyValueStore(TopStreamingArtistByState.STREAM_KTABLE);
        artistStore = testDriver.getKeyValueStore(TopStreamingArtistByState.ARTISTS_KTABLE);
    }

    //=====================================================================================
    // After Each
    //=====================================================================================

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    //=====================================================================================
    // Test 1
    //=====================================================================================

    @Test
    @DisplayName("Stream Table KTable Pipeline")
    public void stream_write_one_then_read_one() {
        // ARRANGE
        String customerId = "customer-1";
        String artistId = "artist-1";


        Stream stream = STREAMS.generate(customerId,artistId);
        log.info("Created stream with id '{}' and value '{}'", stream.id(), stream);

        Artist artist = ARTISTS.generate(artistId);
        log.info("Created artist with id '{}' and value '{}'", artistId, artist);

        // ACT - Input data
        // Note: Order matters! Artist must be processed before stream to ensure the join works
        artistInputTopic.pipeInput(artistId, artist);
        streamInputTopic.pipeInput(stream.id(), stream);

        // ASSERT - Verify stream is stored in KTable
        Stream storedStream = streamStore.get(stream.id());
        log.info("Stream from store has key '{}' and value '{}'", stream.id(), storedStream);
        assertNotNull(storedStream, "Stream should be stored in KTable");
        assertEquals(stream.id(), storedStream.id());

        // Verify artist is stored in KTable
        Artist storedArtist = artistStore.get(artistId);
        log.info("Artist from store has key '{}' and value '{}'", artistId, storedArtist);
        assertNotNull(storedArtist, "Artist should be stored in KTable");
        assertEquals(artistId, storedArtist.id());

        // Verify enriched output
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result = outputTopic.readRecord();
        log.info("Output result has key '{}' and value '{}'", result.key(), result.value());

        // ASSERT - Verify result
        assertEquals(artistId, result.key());
        assertNotNull(result.value(), "Enriched stream should not be null");
        assertEquals(stream.id(), result.value().getStream().id());
        assertEquals(artist.id(), result.value().getArtist().id());
    }

    //=====================================================================================
    // Test 2
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with the Same Artist")
    public void multiple_streams_same_artist() {
        // ARRANGE
        String artistId = "artist-2";
        Artist artist = ARTISTS.generate(artistId);

        String streamId1 = "stream-2a";
        String streamId2 = "stream-2b";
        String customerId1 = "customer-2a";
        String customerId2 = "customer-2b";

        Stream stream1 = STREAMS.generate(customerId1,artistId);
        Stream stream2 = STREAMS.generate(customerId2,artistId);

        // ACT - Input data
        artistInputTopic.pipeInput(artistId, artist);
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);

        // ASSERT - Verify output records
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());

        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());

        assertEquals(0, outputTopic.getQueueSize());
    }


    //=====================================================================================
    // Test 3
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists")
    public void multiple_streams_multiple_artists() {
        // ARRANGE
        // First artist and associated streams
        String artistId1 = "artist-3a";
        Artist artist1 = ARTISTS.generate(artistId1);

        String streamId1 = "stream-3a";
        String streamId2 = "stream-3b";
        String customerId1 = "customer-3a";
        String customerId2 = "customer-3b";

        Stream stream1 = STREAMS.generate(customerId1, artistId1);
        Stream stream2 = STREAMS.generate(customerId2, artistId1);

        // Second artist and associated streams
        String artistId2 = "artist-3c";
        Artist artist2 = ARTISTS.generate(artistId2);

        String streamId3 = "stream-3c";
        String streamId4 = "stream-3d";
        String customerId3 = "customer-3c";
        String customerId4 = "customer-3d";

        Stream stream3 = STREAMS.generate(customerId3, artistId2);
        Stream stream4 = STREAMS.generate(customerId4, artistId2);

        // ACT - Input data
        // Add artists first
        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);

        // Add streams
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);
        streamInputTopic.pipeInput(streamId3, stream3);
        streamInputTopic.pipeInput(streamId4, stream4);

        // ASSERT - Verify output records
        // Validate the stream1 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId1, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());
        assertEquals(artist1.id(), result1.value().getArtist().id());

        // Validate the stream2 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId1, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());
        assertEquals(artist1.id(), result2.value().getArtist().id());

        // Validate the stream3 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result3 = outputTopic.readRecord();
        assertEquals(artistId2, result3.key());
        assertEquals(stream3.id(), result3.value().getStream().id());
        assertEquals(artist2.id(), result3.value().getArtist().id());

        // Validate the stream4 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result4 = outputTopic.readRecord();
        assertEquals(artistId2, result4.key());
        assertEquals(stream4.id(), result4.value().getStream().id());
        assertEquals(artist2.id(), result4.value().getArtist().id());

        assertEquals(0, outputTopic.getQueueSize());
    }

    //=====================================================================================
    // Test 4
    //=====================================================================================

    @Test
    @DisplayName("Multiple Streams with Multiple Artists and Customers")
    public void multiple_streams_multiple_artists_multiple_customers() {
        // ARRANGE
        // First artist and associated streams
        String artistId1 = "artist-3a";
        Artist artist1 = ARTISTS.generate(artistId1);

        String streamId1 = "stream-3a";
        String streamId2 = "stream-3b";
        String customerId1 = "customer-3a";
        String customerId2 = "customer-3b";

        Stream stream1 = STREAMS.generate(customerId1, artistId1);
        Stream stream2 = STREAMS.generate(customerId2, artistId1);

        // Customers
        Customer customer1 = CUSTOMERS.generate(customerId1);
        Customer customer2 = CUSTOMERS.generate(customerId2);

        // Second artist and associated streams
        String artistId2 = "artist-3c";
        Artist artist2 = ARTISTS.generate(artistId2);

        String streamId3 = "stream-3c";
        String streamId4 = "stream-3d";
        String customerId3 = "customer-3c";
        String customerId4 = "customer-3d";

        Stream stream3 = STREAMS.generate(customerId3, artistId2);
        Stream stream4 = STREAMS.generate(customerId4, artistId2);

        // More customers
        Customer customer3 = CUSTOMERS.generate(customerId3);
        Customer customer4 = CUSTOMERS.generate(customerId4);

        // ACT - Input data
        // Add artists first
        artistInputTopic.pipeInput(artistId1, artist1);
        artistInputTopic.pipeInput(artistId2, artist2);

        // Add customers
        customerInputTopic.pipeInput(customerId1, customer1);
        customerInputTopic.pipeInput(customerId2, customer2);
        customerInputTopic.pipeInput(customerId3, customer3);
        customerInputTopic.pipeInput(customerId4, customer4);

        // Add streams
        streamInputTopic.pipeInput(streamId1, stream1);
        streamInputTopic.pipeInput(streamId2, stream2);
        streamInputTopic.pipeInput(streamId3, stream3);
        streamInputTopic.pipeInput(streamId4, stream4);

        // ASSERT - Verify output records for enriched stream
        // Validate the stream1 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result1 = outputTopic.readRecord();
        assertEquals(artistId1, result1.key());
        assertEquals(stream1.id(), result1.value().getStream().id());
        assertEquals(artist1.id(), result1.value().getArtist().id());

        // Validate the stream2 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result2 = outputTopic.readRecord();
        assertEquals(artistId1, result2.key());
        assertEquals(stream2.id(), result2.value().getStream().id());
        assertEquals(artist1.id(), result2.value().getArtist().id());

        // Validate the stream3 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result3 = outputTopic.readRecord();
        assertEquals(artistId2, result3.key());
        assertEquals(stream3.id(), result3.value().getStream().id());
        assertEquals(artist2.id(), result3.value().getArtist().id());

        // Validate the stream4 record
        TestRecord<String, TopStreamingArtistByState.EnrichedStream> result4 = outputTopic.readRecord();
        assertEquals(artistId2, result4.key());
        assertEquals(stream4.id(), result4.value().getStream().id());
        assertEquals(artist2.id(), result4.value().getArtist().id());

        assertEquals(0, outputTopic.getQueueSize());

        // ASSERT - Verify output records for enriched artist customer stream
        // Validate the stream1 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult1 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId1, customerResult1.key());
        assertEquals(stream1.id(), customerResult1.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult1.value().getEnrichedStream().getArtist().id());
        assertEquals(customer1.id(), customerResult1.value().getCustomer().id());

        // Validate the stream2 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult2 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId2, customerResult2.key());
        assertEquals(stream2.id(), customerResult2.value().getEnrichedStream().getStream().id());
        assertEquals(artist1.id(), customerResult2.value().getEnrichedStream().getArtist().id());
        assertEquals(customer2.id(), customerResult2.value().getCustomer().id());

        // Validate the stream3 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult3 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId3, customerResult3.key());
        assertEquals(stream3.id(), customerResult3.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult3.value().getEnrichedStream().getArtist().id());
        assertEquals(customer3.id(), customerResult3.value().getCustomer().id());

        // Validate the stream4 record with customer
        TestRecord<String, TopStreamingArtistByState.EnrichedArtistCustomerStream> customerResult4 =
                enrichedArtistCustomerTopic.readRecord();
        assertEquals(customerId4, customerResult4.key());
        assertEquals(stream4.id(), customerResult4.value().getEnrichedStream().getStream().id());
        assertEquals(artist2.id(), customerResult4.value().getEnrichedStream().getArtist().id());
        assertEquals(customer4.id(), customerResult4.value().getCustomer().id());

        assertEquals(0, enrichedArtistCustomerTopic.getQueueSize());
    }
}