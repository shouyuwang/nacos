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
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.Optional;

/**
 * Instance beat checker for expired instance.
 *
 * <p>Delete the instance if has expired.
 *
 * @author xiweng.yy
 */
public class ExpiredInstanceChecker implements InstanceBeatChecker {
    
    @Override
    public void doCheck(Client client, Service service, HealthCheckInstancePublishInfo instance) {
        // 从配置文件${nacos.naming.expireInstance:true}读取判断是否清除失效的实例
        boolean expireInstance = ApplicationUtils.getBean(GlobalConfig.class).isExpireInstance();
        // 配置判断是否清除失效实例，判断实例是否失效
        if (expireInstance && isExpireInstance(service, instance)) {
            // 摘除实例
            deleteIp(client, service, instance);
        }
    }
    
    private boolean isExpireInstance(Service service, HealthCheckInstancePublishInfo instance) {
        // 获取超时配置
        long deleteTimeout = getTimeout(service, instance);
        // 判断当前时间-最后一次时间 > 超时时间
        return System.currentTimeMillis() - instance.getLastHeartBeatTime() > deleteTimeout;
    }
    
    private long getTimeout(Service service, InstancePublishInfo instance) {
        Optional<Object> timeout = getTimeoutFromMetadata(service, instance);
        if (!timeout.isPresent()) {
            // 从metadata中获取${preserved.ip.delete.timeout}值信息
            timeout = Optional.ofNullable(instance.getExtendDatum().get(PreservedMetadataKeys.IP_DELETE_TIMEOUT));
        }
        // 如果metadata中没有配置，则设置默认超时时间为30秒
        return timeout.map(ConvertUtils::toLong).orElse(Constants.DEFAULT_IP_DELETE_TIMEOUT);
    }
    
    private Optional<Object> getTimeoutFromMetadata(Service service, InstancePublishInfo instance) {
        Optional<InstanceMetadata> instanceMetadata = ApplicationUtils.getBean(NamingMetadataManager.class)
                .getInstanceMetadata(service, instance.getMetadataId());
        // 从metadata中获取${preserved.ip.delete.timeout}超时摘除实例时间配置信息
        return instanceMetadata.map(metadata -> metadata.getExtendData().get(PreservedMetadataKeys.IP_DELETE_TIMEOUT));
    }
    
    private void deleteIp(Client client, Service service, InstancePublishInfo instance) {
        Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.toString(), JacksonUtils.toJson(instance));
        // 从client中移除当前service信息
        client.removeServiceInstance(service);
        // 发送ClientDeregisterServiceEvent事件
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientDeregisterServiceEvent(service, client.getClientId()));
        // 发送InstanceMetadataEvent事件
        NotifyCenter.publishEvent(new MetadataEvent.InstanceMetadataEvent(service, instance.getMetadataId(), true));
    }
}
