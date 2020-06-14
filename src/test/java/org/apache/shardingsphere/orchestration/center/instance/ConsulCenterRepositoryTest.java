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

import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.apache.shardingsphere.orchestration.center.config.CenterConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ConsulCenterRepositoryTest {

    private static ConsulProcess CUNSOL;

    private static final ConsulCenterRepository REPOSITORY = new ConsulCenterRepository();

    @BeforeClass
    public static void init() {
        CUNSOL = ConsulStarterBuilder.consulStarter()
                .withHttpPort(8500)
                .build()
                .start();
        Properties properties = new Properties();
        properties.setProperty(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS.getKey(), String.valueOf(4000L));
        properties.setProperty(ConsulPropertyKey.TOKEN.getKey(), "abcdefgh-12345678-abcdefgh-12345678");
        properties.setProperty(ConsulPropertyKey.NAMESPACE.getKey(), "shardingsphere");
        CenterConfiguration configuration = new CenterConfiguration("zookeeper", properties);
        configuration.setServerLists("http://localhost:8500");
        REPOSITORY.setProps(properties);
        REPOSITORY.init(configuration);
    }

//    @Test
//    public void assertPersist() {
//        CUNSOL
//    }
}