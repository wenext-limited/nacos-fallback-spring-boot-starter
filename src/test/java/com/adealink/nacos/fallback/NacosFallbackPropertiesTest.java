package com.adealink.nacos.fallback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NacosFallbackProperties 单元测试
 */
@DisplayName("NacosFallbackProperties 测试")
class NacosFallbackPropertiesTest {

    private NacosFallbackProperties properties;

    @BeforeEach
    void setUp() {
        properties = new NacosFallbackProperties();
    }

    @Nested
    @DisplayName("默认值测试")
    class DefaultValuesTest {

        @Test
        @DisplayName("enabled 默认值应为 false")
        void enabledDefaultsToFalse() {
            assertFalse(properties.isEnabled());
        }

        @Test
        @DisplayName("localServerAddr 默认值应为 localhost:8848")
        void localServerAddrDefaultsToLocalhost() {
            assertEquals("localhost:8848", properties.getLocalServerAddr());
        }

        @Test
        @DisplayName("localNamespace 默认值应为 null")
        void localNamespaceDefaultsToNull() {
            assertNull(properties.getLocalNamespace());
        }

        @Test
        @DisplayName("localGroup 默认值应为 DEFAULT_GROUP")
        void localGroupDefaultsToDefaultGroup() {
            assertEquals("DEFAULT_GROUP", properties.getLocalGroup());
        }

        @Test
        @DisplayName("testServerAddr 默认值应为 null")
        void testServerAddrDefaultsToNull() {
            // 在 testPublicIp 也为空时应返回 null
            assertNull(properties.getTestServerAddr());
        }

        @Test
        @DisplayName("testNamespace 默认值应为 null")
        void testNamespaceDefaultsToNull() {
            assertNull(properties.getTestNamespace());
        }

        @Test
        @DisplayName("testGroup 默认值应为 DEFAULT_GROUP")
        void testGroupDefaultsToDefaultGroup() {
            assertEquals("DEFAULT_GROUP", properties.getTestGroup());
        }

        @Test
        @DisplayName("testPublicIp 默认值应为 null")
        void testPublicIpDefaultsToNull() {
            assertNull(properties.getTestPublicIp());
        }

        @Test
        @DisplayName("testPrivateIpPrefix 默认值应为 172.")
        void testPrivateIpPrefixDefaultsTo172() {
            assertEquals("172.", properties.getTestPrivateIpPrefix());
        }

        @Test
        @DisplayName("syncIntervalSeconds 默认值应为 60")
        void syncIntervalSecondsDefaultsTo60() {
            assertEquals(60, properties.getSyncIntervalSeconds());
        }

        @Test
        @DisplayName("leaderServiceName 默认值应为 nacos-sync-leader")
        void leaderServiceNameDefaultsToNacosSyncLeader() {
            assertEquals("nacos-sync-leader", properties.getLeaderServiceName());
        }

        @Test
        @DisplayName("leaderCheckIntervalSeconds 默认值应为 10")
        void leaderCheckIntervalSecondsDefaultsTo10() {
            assertEquals(10, properties.getLeaderCheckIntervalSeconds());
        }

        @Test
        @DisplayName("leaderElectionWaitMs 默认值应为 500")
        void leaderElectionWaitMsDefaultsTo500() {
            assertEquals(500, properties.getLeaderElectionWaitMs());
        }
    }

    @Nested
    @DisplayName("getTestServerAddr 方法测试")
    class GetTestServerAddrTest {

        @Test
        @DisplayName("当 testServerAddr 已配置时应返回配置值")
        void shouldReturnConfiguredTestServerAddr() {
            properties.setTestServerAddr("192.168.1.100:8848");
            assertEquals("192.168.1.100:8848", properties.getTestServerAddr());
        }

        @Test
        @DisplayName("当 testServerAddr 为空但 testPublicIp 已配置时应返回 testPublicIp:8848")
        void shouldReturnTestPublicIpWithDefaultPort() {
            properties.setTestPublicIp("10.0.0.1");
            assertEquals("10.0.0.1:8848", properties.getTestServerAddr());
        }

        @Test
        @DisplayName("当 testServerAddr 和 testPublicIp 都为空时应返回 null")
        void shouldReturnNullWhenBothAreEmpty() {
            assertNull(properties.getTestServerAddr());
        }

        @Test
        @DisplayName("testServerAddr 配置优先于 testPublicIp 推导")
        void testServerAddrShouldTakePrecedenceOverTestPublicIp() {
            properties.setTestServerAddr("custom-nacos:8848");
            properties.setTestPublicIp("10.0.0.1");
            assertEquals("custom-nacos:8848", properties.getTestServerAddr());
        }

        @Test
        @DisplayName("当 testServerAddr 为空字符串时应使用 testPublicIp 推导")
        void shouldUseTestPublicIpWhenTestServerAddrIsBlank() {
            properties.setTestServerAddr("");
            properties.setTestPublicIp("10.0.0.2");
            assertEquals("10.0.0.2:8848", properties.getTestServerAddr());
        }

        @Test
        @DisplayName("当 testServerAddr 为只有空格时应使用 testPublicIp 推导")
        void shouldUseTestPublicIpWhenTestServerAddrIsWhitespace() {
            properties.setTestServerAddr("   ");
            properties.setTestPublicIp("10.0.0.3");
            assertEquals("10.0.0.3:8848", properties.getTestServerAddr());
        }
    }

    @Nested
    @DisplayName("属性设置测试")
    class SetterTest {

        @Test
        @DisplayName("应正确设置 enabled")
        void shouldSetEnabled() {
            properties.setEnabled(true);
            assertTrue(properties.isEnabled());
        }

        @Test
        @DisplayName("应正确设置 localServerAddr")
        void shouldSetLocalServerAddr() {
            properties.setLocalServerAddr("192.168.1.1:8848");
            assertEquals("192.168.1.1:8848", properties.getLocalServerAddr());
        }

        @Test
        @DisplayName("应正确设置 localNamespace")
        void shouldSetLocalNamespace() {
            properties.setLocalNamespace("dev");
            assertEquals("dev", properties.getLocalNamespace());
        }

        @Test
        @DisplayName("应正确设置 localGroup")
        void shouldSetLocalGroup() {
            properties.setLocalGroup("CUSTOM_GROUP");
            assertEquals("CUSTOM_GROUP", properties.getLocalGroup());
        }

        @Test
        @DisplayName("应正确设置 testNamespace")
        void shouldSetTestNamespace() {
            properties.setTestNamespace("test");
            assertEquals("test", properties.getTestNamespace());
        }

        @Test
        @DisplayName("应正确设置 testGroup")
        void shouldSetTestGroup() {
            properties.setTestGroup("TEST_GROUP");
            assertEquals("TEST_GROUP", properties.getTestGroup());
        }

        @Test
        @DisplayName("应正确设置 testPublicIp")
        void shouldSetTestPublicIp() {
            properties.setTestPublicIp("203.0.113.1");
            assertEquals("203.0.113.1", properties.getTestPublicIp());
        }

        @Test
        @DisplayName("应正确设置 testPrivateIpPrefix")
        void shouldSetTestPrivateIpPrefix() {
            properties.setTestPrivateIpPrefix("10.");
            assertEquals("10.", properties.getTestPrivateIpPrefix());
        }

        @Test
        @DisplayName("应正确设置 syncIntervalSeconds")
        void shouldSetSyncIntervalSeconds() {
            properties.setSyncIntervalSeconds(120);
            assertEquals(120, properties.getSyncIntervalSeconds());
        }

        @Test
        @DisplayName("应正确设置 leaderServiceName")
        void shouldSetLeaderServiceName() {
            properties.setLeaderServiceName("custom-leader");
            assertEquals("custom-leader", properties.getLeaderServiceName());
        }

        @Test
        @DisplayName("应正确设置 leaderCheckIntervalSeconds")
        void shouldSetLeaderCheckIntervalSeconds() {
            properties.setLeaderCheckIntervalSeconds(30);
            assertEquals(30, properties.getLeaderCheckIntervalSeconds());
        }

        @Test
        @DisplayName("应正确设置 leaderElectionWaitMs")
        void shouldSetLeaderElectionWaitMs() {
            properties.setLeaderElectionWaitMs(1000);
            assertEquals(1000, properties.getLeaderElectionWaitMs());
        }
    }
}
