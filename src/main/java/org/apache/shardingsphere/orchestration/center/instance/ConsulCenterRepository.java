/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.orchestration.center.instance;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.cache.KVCache;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.DeleteOptions;
import com.orbitz.consul.option.ImmutablePutOptions;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.PutOptions;
import com.orbitz.consul.option.QueryOptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.orchestration.center.ConfigCenterRepository;
import org.apache.shardingsphere.orchestration.center.config.CenterConfiguration;
import org.apache.shardingsphere.orchestration.center.exception.OrchestrationException;
import org.apache.shardingsphere.orchestration.center.instance.option.CustomDeleteOptions;
import org.apache.shardingsphere.orchestration.center.listener.DataChangedEvent;
import org.apache.shardingsphere.orchestration.center.listener.DataChangedEventListener;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.apache.shardingsphere.orchestration.center.instance.ConsulPropertyKey.TOKEN;

@Slf4j
public class ConsulCenterRepository implements ConfigCenterRepository {

    private ConsulProperties consulProperties;

    @Getter
    @Setter
    private Properties props = new Properties();

    private Consul client;

    private KeyValueClient kvClient;

    private QueryOptions queryOptions;

    private PutOptions putOptions;

    private DeleteOptions deleteOptions;

    private ConcurrentMap<String, Long> modifyIndexes;

    private ConcurrentMap<String, KVCache> kvCaches;

    private String namespace;

    @Override
    public void init(CenterConfiguration config) {
        this.consulProperties = new ConsulProperties(props);
        this.namespace = config.getNamespace();
        List<String> serviceList = Splitter.on(",").trimResults().splitToList(config.getServerLists());
        if (CollectionUtils.isEmpty(serviceList)) {
            throw new OrchestrationException("No configuration of service list of consul");
        }
        List<HostAndPort> hostAndPorts = serviceList.stream().map(HostAndPort::fromString).collect(Collectors.toList());
        if (hostAndPorts.size() < 2) {
            this.client = Consul.builder().withHostAndPort(hostAndPorts.get(0)).build();
        } else {
            this.client = Consul.builder()
                    .withMultipleHostAndPort(hostAndPorts, consulProperties.getValue(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS))
                    .build();
        }
        this.kvClient = client.keyValueClient();
        this.queryOptions = getQueryOptions();
        this.putOptions = getPutOptions();
        this.deleteOptions = getDeleteOptions();
        this.modifyIndexes = new ConcurrentHashMap<>();
        this.kvCaches = new ConcurrentHashMap<>();
    }

    @Override
    public String get(String key) {
        if (StringUtils.isNotBlank(key)) {
            key = namespace + "/" + key;
        }
        return kvClient.getValue(key, queryOptions)
                .flatMap(value -> value.getValueAsString(Charsets.UTF_8))
                .orElse(null);
    }

    @Override
    public void persist(String key, String value) {
        if (StringUtils.isNotBlank(namespace)) {
            key = namespace + "/" + key;
        }
        kvClient.putValue(key, value, 0L, putOptions, Charsets.UTF_8);
    }

    @Override
    public void close() {
        client.destroy();
        kvCaches.values().forEach(kvCache -> kvCache.stop());
        kvCaches.clear();
    }

    @Override
    public List<String> getChildrenKeys(String key) {
        if (StringUtils.isNotBlank(namespace)) {
            key = namespace + "/" + key;
        }
        return kvClient.getKeys(key, queryOptions);
    }

    @Override
    public void watch(String key, DataChangedEventListener dataChangedEventListener) {
        String originKey = key;
        if (StringUtils.isNotBlank(namespace)) {
            key = namespace + "/" + key;
        }
        kvCaches.computeIfAbsent(key, path -> {
            KVCache kvCache = KVCache.newCache(kvClient, path);
            kvCache.addListener(newValues -> {
                Optional<Value> newValue = newValues.values().stream()
                        .filter(value -> path.equals(value.getKey()))
                        .findAny();
                if (newValue.isPresent()) {
                    if (!modifyIndexes.containsKey(path)) {
                        modifyIndexes.put(path, newValue.get().getModifyIndex());
                        dataChangedEventListener.onChange(new DataChangedEvent(originKey, newValue.get().getValueAsString(Charsets.UTF_8).orElse(null), DataChangedEvent.ChangedType.ADDED));
                    } else if (!modifyIndexes.get(path).equals(newValue.get().getModifyIndex())) {
                        modifyIndexes.put(path, newValue.get().getModifyIndex());
                        dataChangedEventListener.onChange(new DataChangedEvent(originKey, newValue.get().getValueAsString(Charsets.UTF_8).orElse(null), DataChangedEvent.ChangedType.UPDATED));
                    }
                } else if (modifyIndexes.containsKey(path)){
                    modifyIndexes.remove(path);
                    dataChangedEventListener.onChange(new DataChangedEvent(originKey, null, DataChangedEvent.ChangedType.DELETED));
                }
            });
            kvCache.start();
            return kvCache;
        });
    }

    @Override
    public void delete(String key) {
        key = getPath(key);
        kvClient.deleteKey(key, new CustomDeleteOptions(consulProperties.getValue(TOKEN)));
    }

    @Override
    public String getType() {
        return "consul";
    }

    private QueryOptions getQueryOptions() {
        ImmutableQueryOptions.Builder builder = ImmutableQueryOptions.builder();
        if (StringUtils.isNotBlank(consulProperties.getValue(TOKEN))) {
            builder.token(consulProperties.getValue(TOKEN).toString());
        }
        return builder.build();
    }

    private PutOptions getPutOptions() {
        ImmutablePutOptions.Builder builder = ImmutablePutOptions.builder();
        String token = consulProperties.getValue(TOKEN);
        if (StringUtils.isNotBlank(token)) {
            builder.token(token);
        }
        return builder.build();
    }

    private DeleteOptions getDeleteOptions() {
        String token = consulProperties.getValue(TOKEN);
        if (StringUtils.isNotBlank(token)) {
            return new CustomDeleteOptions(token);
        }
        return DeleteOptions.BLANK;
    }

    private String getPath(String key) {
        if (StringUtils.isNotBlank(namespace)) {
            return namespace + "/" + key;
        }
        return key;
    }
}
