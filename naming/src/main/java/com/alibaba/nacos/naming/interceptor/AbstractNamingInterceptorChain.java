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

package com.alibaba.nacos.naming.interceptor;

import com.alibaba.nacos.common.spi.NacosServiceLoader;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract Naming Interceptor Chain.
 *
 * @author xiweng.yy
 */
public abstract class AbstractNamingInterceptorChain<T extends Interceptable>
        implements NacosNamingInterceptorChain<T> {
    
    private final List<NacosNamingInterceptor<T>> interceptors;
    
    protected AbstractNamingInterceptorChain(Class<? extends NacosNamingInterceptor<T>> clazz) {
        this.interceptors = new LinkedList<>();
        // spi加载
        interceptors.addAll(NacosServiceLoader.load(clazz));
        // 排序
        interceptors.sort(Comparator.comparingInt(NacosNamingInterceptor::order));
    }
    
    /**
     * Get all interceptors.
     *
     * @return interceptors list
     */
    protected List<NacosNamingInterceptor<T>> getInterceptors() {
        return interceptors;
    }
    
    @Override
    public void addInterceptor(NacosNamingInterceptor<T> interceptor) {
        interceptors.add(interceptor);
        interceptors.sort(Comparator.comparingInt(NacosNamingInterceptor::order));
    }
    
    @Override
    public void doInterceptor(T object) {
        // 循环执行任务链
        for (NacosNamingInterceptor<T> each : interceptors) {
            // 当interceptors为心跳检测的时候，判断class是否为InstanceBeatCheckTask
            if (!each.isInterceptType(object.getClass())) {
                continue;
            }
            // 处理判断intercept
            if (each.intercept(object)) {
                object.afterIntercept();
                return;
            }
        }
        // 通过intercept
        object.passIntercept();
    }
}
