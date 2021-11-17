/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck.heartbeat;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.Optional;

/**
 * Instance beat checker for unhealthy instances.
 *
 * <p>Mark these instances healthy status {@code false} if beat time out.
 *
 * @author xiweng.yy
 */
public class UnhealthyInstanceChecker implements InstanceBeatChecker {
    
    // 健康监测
    @Override
    public void doCheck(Client client, Service service, HealthCheckInstancePublishInfo instance) {
        // 判断实例是否为health状态，是否可以修改实例为非健康状态
        if (instance.isHealthy() && isUnhealthy(service, instance)) {
            // 更改健康状态
            changeHealthyStatus(client, service, instance);
        }
    }
    
    private boolean isUnhealthy(Service service, HealthCheckInstancePublishInfo instance) {
        // 获取超时时间
        long beatTimeout = getTimeout(service, instance);
        // 当前时间-最后一次心跳时间上报时间 > 超时时间
        return System.currentTimeMillis() - instance.getLastHeartBeatTime() > beatTimeout;
    }
    
    private long getTimeout(Service service, InstancePublishInfo instance) {
        // 获取metadata中的超时时间配置
        Optional<Object> timeout = getTimeoutFromMetadata(service, instance);
        if (!timeout.isPresent()) {
            // 取超时时间
            timeout = Optional.ofNullable(instance.getExtendDatum().get(PreservedMetadataKeys.HEART_BEAT_TIMEOUT));
        }
        // metadata中没有配置timeout的话，进行取默认的超时时间（15秒）
        return timeout.map(ConvertUtils::toLong).orElse(Constants.DEFAULT_HEART_BEAT_TIMEOUT);
    }
    
    private Optional<Object> getTimeoutFromMetadata(Service service, InstancePublishInfo instance) {
        Optional<InstanceMetadata> instanceMetadata = ApplicationUtils.getBean(NamingMetadataManager.class)
                .getInstanceMetadata(service, instance.getMetadataId());
        // 从metadata中获取超时时间的配置信息
        return instanceMetadata.map(metadata -> metadata.getExtendData().get(PreservedMetadataKeys.HEART_BEAT_TIMEOUT));
    }
    
    private void changeHealthyStatus(Client client, Service service, HealthCheckInstancePublishInfo instance) {
        // 设置实例的状态为非健康
        instance.setHealthy(false);
        Loggers.EVT_LOG
                .info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client last beat: {}", instance.getIp(),
                        instance.getPort(), instance.getCluster(), service.getName(), UtilsAndCommons.LOCALHOST_SITE,
                        instance.getLastHeartBeatTime());
        // 发布ServiceChangedEvent事件
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service));
        // 发布ClientChangedEvent事件
        NotifyCenter.publishEvent(new ClientEvent.ClientChangedEvent(client));
    }
}
