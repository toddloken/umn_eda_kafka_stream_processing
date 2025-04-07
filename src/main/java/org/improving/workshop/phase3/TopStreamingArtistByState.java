package org.improving.workshop.phase3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

//===============================================================================================================
// Topics Ktables and Serdes
//===============================================================================================================

@Slf4j
public class TopStreamingArtistByState {
    // Reference TOPIC_DATA_DEMO_* properties in Streams
    public static final String ADDRESS_INPUT_TOPIC = TOPIC_DATA_DEMO_ADDRESSES;
    public static final String CUSTOMER_INPUT_TOPIC = TOPIC_DATA_DEMO_CUSTOMERS;
    public static final String ARTIST_INPUT_TOPIC = TOPIC_DATA_DEMO_ARTISTS;
    public static final String STREAM_INPUT_TOPIC = TOPIC_DATA_DEMO_STREAMS;


    // KTABLE DEFINITIONS MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String ADDRESS_KTABLE = "kafka-workshop-address-ktable";
    public static final String CUSTOMER_ADDRESS_KTABLE = "kafka-workshop-customer-address-ktable";
    public static final String CUSTOMER_ADDRESS_STREAM_KTABLE = "kafka-workshop-customer-address-stream-ktable";
    public static final String OUTPUT_TOPIC = "kafka-workshop-top-streaming-artist-by-state";

    // Serdes
    public static final JsonSerde<CustomerAddress> CUSTOMER_ADDRESS_JSON_SERDE = new JsonSerde<>(CustomerAddress.class);
    public static final JsonSerde<CustomerAddressStream> CUSTOMER_ADDRESS_STREAM_JSON_SERDE = new JsonSerde<>(CustomerAddressStream.class);
    public static final JsonSerde<EnrichedStream> ENRICHED_TICKET_JSON_SERDE = new JsonSerde<>(EnrichedStream.class);
    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);


    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
            = new JsonSerde<>(
            new TypeReference<LinkedHashMap<String, Long>>() {
            },
            new ObjectMapper()
                    .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );

    // ===============================================================================================================
    // Main
    // ===============================================================================================================

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }


    //===============================================================================================================
    // Topology
    //===============================================================================================================

    static void configureTopology(final StreamsBuilder builder) {

        //======================
        // KTables
        //=======================
        // Address
        KTable<String, Address> addressTable = builder
                .table(
                        ADDRESS_INPUT_TOPIC,
                        Materialized
                                .<String, Address>as(persistentKeyValueStore(ADDRESS_KTABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );
        addressTable
                .toStream()
                .peek((key, artist) -> log.info("Address '{}' registered with value '{}'", key, artist))
                .to(ADDRESS_KTABLE, Produced.with(Serdes.String(), SERDE_ADDRESS_JSON));

        // CustomerAddress
        KTable<String, CustomerAddress> customerAddressKTable = builder
                .stream(CUSTOMER_INPUT_TOPIC, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))
                .peek((customerId, customer) -> log.info("Customer '{}' registered with value '{}'", customerId, customer))

                .selectKey((customerId, customer) -> customer.id(), Named.as("rekey-customer-by-id"))

                // Join to the address KTable. Causes a repartition
                .join(
                        addressTable, // Join to table
                        (customer, address)
                                -> new CustomerAddress(customer,address)
                )

                .peek((key, value)-> log.info("CustomerArtist with CustomerId '{}' and addressId '{}' registered with value '{}'",value.customer.id(), value.address.id(), value))

                //rekey
                .selectKey((key, customerAddress)
                        -> customerAddress.customer.id(), Named.as("rekey-customerAddress-by-customerid"))

                .toTable(
                        Materialized
                                .<String, CustomerAddress>as(persistentKeyValueStore(CUSTOMER_ADDRESS_KTABLE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CUSTOMER_ADDRESS_JSON_SERDE)
                );
        customerAddressKTable
                .toStream()
                .peek((key, customerAddress) -> log.info("CustomerAddress '{}' registered with value '{}'", key, customerAddress))
                .to(CUSTOMER_ADDRESS_KTABLE, Produced.with(Serdes.String(),CUSTOMER_ADDRESS_JSON_SERDE));



        //=============================
        // builder
        //=============================
        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))

                // rekey so that the groupBy is by customerid and not streamid
                // groupBy is shorthand for (selectKey + groupByKey)
                .groupBy((k, v) -> v.customerid())

                // keep track of each customer's artist stream counts in a ktable
                .aggregate(
                        // initializer
                        SortedCounterMap::new,

                        // aggregator
                        (customerId, stream, customerArtistStreamCounts) -> {
                            customerArtistStreamCounts.incrementCount(stream.artistid());
                            return customerArtistStreamCounts;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("customer-artist-stream-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(COUNTER_MAP_JSON_SERDE)
                )

                // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
                .toStream()
                // trim to only the top 3
                .mapValues(sortedCounterMap -> sortedCounterMap.top(10))
                .peek((key, counterMap) -> log.info("Customer {}'s Top 10 Streamed Artists: {}", key, counterMap))
                // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));
    }

    //===============================================================================================================
    // Data
    //===============================================================================================================
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddress {
        public Customer customer;
        public Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddressStream {
        public Stream stream;
        public Customer customer;
        public Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EnrichedStream {
        public Address address;
        public Customer customer;
        public Stream stream;
        public Artist artist
    }

    //===============================================================================================================
    // Not sure if this will still be needed - but keep for nwo
    //===============================================================================================================

    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private LinkedHashMap<String, Long> map;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        public void incrementCount(String id) {
            map.compute(id, (k, v) -> v == null ? 1 : v + 1);

            // replace with sorted map
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    // keep a limit on the map size
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
        public LinkedHashMap<String, Long> top(int limit) {
            return map.entrySet().stream()
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }
}