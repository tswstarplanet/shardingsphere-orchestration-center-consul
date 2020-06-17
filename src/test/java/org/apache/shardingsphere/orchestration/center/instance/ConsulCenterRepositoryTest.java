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
import org.apache.shardingsphere.orchestration.center.listener.DataChangedEvent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class ConsulCenterRepositoryTest {

    private static ConsulProcess CUNSOL;

    private static final ConsulCenterRepository REPOSITORY = new ConsulCenterRepository();

    @BeforeClass
    public static void init() {
        CUNSOL = ConsulStarterBuilder.consulStarter()
                .withHttpPort(8601)
                .build()
                .start();
        Properties properties = new Properties();
        properties.setProperty(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS.getKey(), String.valueOf(4000L));
        properties.setProperty(ConsulPropertyKey.TOKEN.getKey(), "abcdefgh-12345678-abcdefgh-12345678");
        CenterConfiguration configuration = new CenterConfiguration("zookeeper", properties);
        configuration.setServerLists("localhost:8601");
        configuration.setNamespace("shardingsphere");
        REPOSITORY.setProps(properties);
        REPOSITORY.init(configuration);
    }

    @Test
    public void assertPersist() {
        REPOSITORY.persist("test", "value1");
        Assert.assertEquals("value1", REPOSITORY.get("test"));
    }

    @Test
    public void assertUpdate() {
        REPOSITORY.persist("test", "value2");
        assertThat(REPOSITORY.get("test"), is("value2"));
        REPOSITORY.persist("test", "value3");
        assertThat(REPOSITORY.get("test"), is("value3"));
    }

    @Test
    public void assertGetChildrenKeys() {
        REPOSITORY.persist("test/children/keys/1", "value11");
        REPOSITORY.persist("test/children/keys/2", "value12");
        REPOSITORY.persist("test/children/keys/3", "value13");
        List<String> childrenKeys = REPOSITORY.getChildrenKeys("test/children/keys");
        assertThat(childrenKeys.size(), is(3));
    }

    @Test
    public void assertWatchAddedChangedType() throws InterruptedException {
        AtomicReference<DataChangedEvent> actualDataChangedEvent = new AtomicReference<>();
        REPOSITORY.watch("test/children_added", actualDataChangedEvent::set);
        REPOSITORY.persist("test/children_added", "value4");
        Thread.sleep(50L);
        DataChangedEvent event = actualDataChangedEvent.get();
        assertNotNull(event);
        assertThat(event.getChangedType(), is(DataChangedEvent.ChangedType.ADDED));
        assertThat(event.getKey(), is("test/children_added"));
        assertThat(event.getValue(), is("value4"));
    }

    @Test
    public void assertWatchUpdatedChangedType() throws Exception {
        REPOSITORY.persist("test/children_updated/1", "value1");
        AtomicReference<DataChangedEvent> dataChangedEventActual = new AtomicReference<>();
        REPOSITORY.watch("test/children_updated/1", dataChangedEventActual::set);
        REPOSITORY.persist("test/children_updated/1", "value2");
        Thread.sleep(50L);
        DataChangedEvent dataChangedEvent = dataChangedEventActual.get();
        assertNotNull(dataChangedEvent);
        assertThat(dataChangedEvent.getChangedType(), is(DataChangedEvent.ChangedType.UPDATED));
        assertThat(dataChangedEvent.getKey(), is("test/children_updated/1"));
        assertThat(dataChangedEvent.getValue(), is("value2"));
        assertThat(REPOSITORY.get("test/children_updated/1"), is("value2"));
    }

    @Test
    public void assertWatchDeletedChangedType() throws Exception {
        REPOSITORY.persist("test/children_deleted/1", "value1");
        AtomicReference<DataChangedEvent> dataChangedEventActual = new AtomicReference<>();
        REPOSITORY.watch("test/children_deleted/1", dataChangedEventActual::set);
        REPOSITORY.delete("test/children_deleted/1");
        Thread.sleep(50L);
        DataChangedEvent dataChangedEvent = dataChangedEventActual.get();
        assertNotNull(dataChangedEvent);
        assertThat(dataChangedEvent.getChangedType(), is(DataChangedEvent.ChangedType.DELETED));
        assertThat(dataChangedEvent.getKey(), is("test/children_deleted/1"));
        assertEquals(dataChangedEvent.getValue(), null);
        assertEquals(REPOSITORY.get("test/children_updated/1"), null);
    }

    @Test
    public void assertGetWithNonExistentKey() {
        assertNull(REPOSITORY.get("/test/nonExistentKey"));
    }

    @Test
    public void assertDelete() {
        REPOSITORY.persist("test/children/1", "value1");
        REPOSITORY.persist("test/children/2", "value2");
        assertThat(REPOSITORY.get("test/children/1"), is("value1"));
        assertThat(REPOSITORY.get("test/children/2"), is("value2"));
        REPOSITORY.delete("test/children");
        assertNull(REPOSITORY.get("test/children/1"));
        assertNull(REPOSITORY.get("test/children/2"));
    }

    @Test
    public void assertZClose() {
        REPOSITORY.close();
    }
}