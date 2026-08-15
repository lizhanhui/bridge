package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class SQLTransformTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FILTER_SQL =
        "SELECT * FROM payload WHERE payload.age < 30 and payload.address.number = 555";

    private static JsonNode json(String s) throws Exception {
        return MAPPER.readTree(s);
    }

    @Test
    void matchingRecordReturnsRowWithKeyAndTimestampPreserved() throws Exception {
        SQLTransform<String, String> transform = new SQLTransform<>(FILTER_SQL);
        Record<String, String> input = new Record<>("key-1",
            "{\"id\": \"3\", \"age\": 25, \"address\": {\"number\": 555, \"street\": \"1st street\"}}", 42L);

        Optional<List<Record<String, String>>> result = transform.transform(input);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        Record<String, String> out = result.get().get(0);
        assertEquals("key-1", out.key());
        assertEquals(42L, out.timestamp());
        assertEquals(json(input.value()), json(out.value()));
    }

    @Test
    void nonMatchingRecordIsFilteredOut() {
        SQLTransform<String, String> transform = new SQLTransform<>(FILTER_SQL);
        Record<String, String> input = new Record<>("key-1", "{\"id\": \"1\", \"age\": 32}", 42L);

        assertTrue(transform.transform(input).isEmpty());
    }

    @Test
    void malformedJsonIsDropped() {
        SQLTransform<String, String> transform = new SQLTransform<>(FILTER_SQL);
        Record<String, String> input = new Record<>("key-1", "{not json", 42L);

        assertTrue(transform.transform(input).isEmpty());
    }

    @Test
    void projectionReturnsOnlySelectedFields() throws Exception {
        SQLTransform<String, String> transform = new SQLTransform<>(
            "SELECT payload.name, payload.age FROM payload WHERE payload.age < 30");
        Record<String, String> input = new Record<>("key-1",
            "{\"name\": \"person_2\", \"age\": 24, \"address\": \"555 1st street\"}", 42L);

        Optional<List<Record<String, String>>> result = transform.transform(input);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        assertEquals(json("{\"name\": \"person_2\", \"age\": 24}"), json(result.get().get(0).value()));
    }

    @Test
    void unnestProducesOneRecordPerRow() {
        SQLTransform<String, String> transform = new SQLTransform<>(
            "SELECT VALUE t FROM payload.tags AS t");
        Record<String, String> input = new Record<>("key-1",
            "{\"id\": \"3\", \"tags\": [\"premium_user\", \"beta_tester\"]}", 42L);

        Optional<List<Record<String, String>>> result = transform.transform(input);

        assertTrue(result.isPresent());
        assertEquals(2, result.get().size());
        assertEquals("\"premium_user\"", result.get().get(0).value());
        assertEquals("\"beta_tester\"", result.get().get(1).value());
        assertTrue(result.get().stream().allMatch(r -> "key-1".equals(r.key())));
    }

    @Test
    void invalidSqlFailsFastInConstructor() {
        assertThrows(RuntimeException.class, () -> new SQLTransform<>("SELECT FROM WHERE"));
    }
}
