package com.tencent.cloud.mqtt.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MqttClientsTest {

    @Test
    void parsesHostAndPort() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com:1883");
        assertEquals("mqtt.example.com", hp.host());
        assertEquals(1883, hp.port());
        assertFalse(hp.ssl());
    }

    @Test
    void port8883EnablesSsl() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com:8883");
        assertEquals(8883, hp.port());
        assertTrue(hp.ssl());
    }

    @Test
    void defaultsToPort1883WithoutSsl() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com");
        assertEquals("mqtt.example.com", hp.host());
        assertEquals(1883, hp.port());
        assertFalse(hp.ssl());
    }

    @Test
    void rejectsMalformedPort() {
        assertThrows(IllegalArgumentException.class, () -> MqttClients.parseAccessPoint("host:abc"));
    }
}
