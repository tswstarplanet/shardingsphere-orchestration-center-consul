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

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.apache.shardingsphere.orchestration.center.instance.ConsulPropertyKey.TOKEN;

public class ConsulPropertiesTest {
    @Test
    public void assertGetValue() {
        Properties props = new Properties();
        props.setProperty(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS.getKey(), "4000L");
        props.setProperty(TOKEN.getKey(), "abcdefgh-12345678-abcdefgh-12345678");
        ConsulProperties actual = new ConsulProperties(props);
        Assert.assertEquals(actual.getValue(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS), Long.valueOf(4000L));
        Assert.assertEquals(actual.getValue(TOKEN), "abcdefgh-12345678-abcdefgh-12345678");
    }

    @Test
    public void assertGetDefaultValue() {
        Properties props = new Properties();
        ConsulProperties actual = new ConsulProperties(props);
        Assert.assertEquals(actual.getValue(ConsulPropertyKey.BLACKLIST_TIME_IN_MILLIS), Long.valueOf(3000L));
        Assert.assertEquals(actual.getValue(TOKEN), "");
    }
}