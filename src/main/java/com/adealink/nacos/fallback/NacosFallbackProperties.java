package com.adealink.nacos.fallback;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;

/**
 * Nacos Fallback 配置属性
 * <p>用于配置本地开发环境与测试环境 Nacos 之间的服务同步</p>
 *
 * @author suyihang
 * @since 1.0.0
 */
@Data
@ConfigurationProperties(prefix = "nacos.fallback")
@Validated
public class NacosFallbackProperties {

    /**
     * 是否启用 fallback 机制
     */
    private boolean enabled = false;

    /**
     * 本地 Nacos 服务地址
     */
    @NotBlank(message = "本地 Nacos 地址不能为空")
    private String localServerAddr = "localhost:8848";

    /**
     * 本地 Nacos 命名空间
     */
    private String localNamespace;

    /**
     * 本地 Nacos 分组
     */
    private String localGroup = "DEFAULT_GROUP";

    /**
     * 测试环境 Nacos 服务地址
     * <p>
     * 如果未配置，默认使用 testPublicIp:8848
     */
    private String testServerAddr;

    /**
     * 测试环境 Nacos 命名空间
     */
    private String testNamespace;

    /**
     * 测试环境 Nacos 分组
     */
    private String testGroup = "DEFAULT_GROUP";

    /**
     * 测试环境公网 IP
     */
    @NotBlank(message = "测试环境公网 IP 不能为空")
    private String testPublicIp;

    /**
     * 测试环境内网 IP 前缀(用于识别需要替换的 IP)
     */
    private String testPrivateIpPrefix = "172.";

    /**
     * 同步间隔(秒)
     */
    @Positive(message = "同步间隔必须大于 0")
    private long syncIntervalSeconds = 60;

    /**
     * Leader 选举服务名称
     */
    private String leaderServiceName = "nacos-sync-leader";

    /**
     * Leader 检查间隔(秒)
     */
    @Positive(message = "Leader 检查间隔必须大于 0")
    private long leaderCheckIntervalSeconds = 10;

    /**
     * Leader 选举等待时间(毫秒)
     * 用于处理多个实例同时竞选 Leader 的竞争条件
     */
    @Positive(message = "Leader 选举等待时间必须大于 0")
    private long leaderElectionWaitMs = 500;

    /**
     * 获取测试环境 Nacos 服务地址
     * <p>
     * 如果未配置 testServerAddr，默认使用 testPublicIp:8848
     */
    public String getTestServerAddr() {
        if (StringUtils.hasText(testServerAddr)) {
            return testServerAddr;
        }
        if (StringUtils.hasText(testPublicIp)) {
            return testPublicIp + ":8848";
        }
        return null;
    }
}
