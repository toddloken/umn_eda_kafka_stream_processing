package org.improving.workshop.phase3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
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

import static org.improving.workshop.utils.DataFaker.ADDRESSES;
import static org.improving.workshop.utils.DataFaker.CUSTOMERS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class TopStreamingArtistByStateTest {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;

    private TestOutputTopic<String, Address> addressKTable;
    private TestOutputTopic<String, TopStreamingArtistByState.CustomerAddress> customerAddressKTable;
    private TestOutputTopic<String,TopStreamingArtistByState.CustomerAddressStream> customerAddressStreamKTable;
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    // ====================================================================
    // Before Each
    // ====================================================================

    @BeforeEach
    public void setup() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        TopStreamingArtistByState.configureTopology(streamsBuilder);

        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
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

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        );

        addressKTable = driver.createOutputTopic(
                TopStreamingArtistByState.ADDRESS_KTABLE,
                Serdes.String().deserializer(),
                Streams.SERDE_ADDRESS_JSON.deserializer()
        );

        customerAddressKTable = driver.createOutputTopic(
                TopStreamingArtistByState.CUSTOMER_ADDRESS_KTABLE,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.CUSTOMER_ADDRESS_JSON_SERDE.deserializer()
        );

        customerAddressStreamKTable = driver.createOutputTopic(
                TopStreamingArtistByState.CUSTOMER_ADDRESS_STREAM_KTABLE,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.CUSTOMER_ADDRESS_STREAM_JSON_SERDE.deserializer()
        );


        outputTopic = driver.createOutputTopic(
                TopStreamingArtistByState.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamingArtistByState.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        );
    }

    // ====================================================================
    // After Each
    // ====================================================================

    @AfterEach
    public void cleanup() {
        driver.close();
    }


    // ====================================================================
    // Test one
    // ====================================================================

    @Test
    @DisplayName("Check Address KTable Pipeline")
    public void address_ktable_write_one_then_read_one() {
        // ARRANGE
        String addressId1 = "address-1";
        Address address1 = ADDRESSES.generateCustomerAddress(addressId1);
        log.info("Created Address with id '{}' and value '{}'", address1, addressId1);

        // ACT - First batch of stream events
        // Inputs
        addressInputTopic.pipeInput(addressId1, address1);
        // Outputs
        KeyValue<String, Address> result = addressKTable.readKeyValue();
        log.info("Output result has key '{}' and value '{}'", result.key, result.value);

        // ASSERT - Verify initial top artists state
        assertEquals(addressId1,result.key);
    }

    // ====================================================================
    // Test Two
    // ====================================================================

    @Test
    @DisplayName("Check CustomerAddress KTable Pipeline")
    public void customer_address_ktable_write_one_then_read_one() {
        // ARRANGE
        String addressId1 = "address-1";
        Address address1 = ADDRESSES.generateCustomerAddress(addressId1);
        log.info("Created Address with id '{}' and value '{}'", address1, addressId1);

        String customerID1 = "customer-1";
        Customer customer1 = CUSTOMERS.generate(customerID1);
        log.info("Created Customer with id '{}' and value '{}'", customer1, customerID1);


        // ACT - First batch of stream events
        // Inputs
        addressInputTopic.pipeInput(addressId1,address1);
        customerInputTopic.pipeInput(customerID1, customer1);

        // Outputs
        KeyValue<String, Address> result = addressKTable.readKeyValue();

        // ASSERT - Verify initial top artists state
        assertEquals(addressId1,result.key);
    }

    // ====================================================================
    // Test Two
    // ====================================================================

}