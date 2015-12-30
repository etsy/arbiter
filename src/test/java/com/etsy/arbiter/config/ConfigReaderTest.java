/*
 * Copyright 2015-2016 Etsy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.etsy.arbiter.config;

import com.etsy.arbiter.util.YamlReader;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ConfigReaderTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private YamlReader<Config> reader;

    @Before
    public void setup() {
        reader = new YamlReader<>(Config.getYamlConstructor());
    }

    @Test
    public void testRead() {
        Config actual = reader.read(getClass().getClassLoader().getResource("testconfig.yaml"));

        Config expected = new Config();
        expected.setKillName("kill");
        expected.setKillMessage("message");
        ActionType expectedTestAction = new ActionType();
        expected.setActionTypes(Collections.singletonList(expectedTestAction));

        expectedTestAction.setTag("testaction");
        expectedTestAction.setName("test");
        expectedTestAction.setXmlns("uri:oozie:test-action:0.1");
        Map<String, List<String>> defaultArgs = new HashMap<>();
        defaultArgs.put("a", Lists.newArrayList("a", "b", "c"));
        expectedTestAction.setDefaultArgs(defaultArgs);
        Map<String, String> properties = new HashMap<>();
        properties.put("p1", "v1");
        properties.put("p2", "v2");
        expectedTestAction.setConfigurationPosition(1);
        expectedTestAction.setProperties(properties);

        assertEquals(expected, actual);
    }

    @Test
    public void testReadNonExistentResource() {
        exception.expect(NullPointerException.class);
        reader.read(getClass().getClassLoader().getResource("nonexistent.yaml"));
    }

    @Test
    public void testReadNonExistentFile() {
        exception.expect(RuntimeException.class);
        reader.read(new File("nonexistent.yaml"));
    }
}