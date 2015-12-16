/*
 * Copyright 2015 Etsy
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

import com.etsy.arbiter.exception.ConfigurationException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.junit.Assert.*;

public class ConfigurationMergerTest {
    public static final String NAME = "name";
    public static final String TAG = "tag";
    public static final String XLMNS = "xlmns";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private List<ActionType> actionTypes;

    @Before
    public void setup() {
        ActionType a1 = new ActionType();
        Map<String, List<String>> defaultArgs = new HashMap<>();
        defaultArgs.put("a", Lists.newArrayList("a", "b", "c"));
        a1.setDefaultArgs(defaultArgs);
        Map<String, String> properties = new HashMap<>();
        properties.put("p1", "v1");
        properties.put("p2", "v2");
        a1.setProperties(properties);
        a1.setLowPrecedence(false);
        a1.setName(NAME);
        a1.setTag(TAG);
        a1.setXmlns(XLMNS);

        ActionType a2 = new ActionType();
        defaultArgs = new HashMap<>();
        defaultArgs.put("a", Lists.newArrayList("d", "c"));
        a2.setDefaultArgs(defaultArgs);
        properties = new HashMap<>();
        properties.put("p3", "v3");
        properties.put("p2", "v1");
        a2.setProperties(properties);
        a2.setLowPrecedence(true);
        a2.setName(NAME);
        a2.setTag(TAG);
        a2.setXmlns(XLMNS);

        actionTypes = Arrays.asList(a1, a2);
    }

    @Test
    public void testMismatchXlmns() throws ConfigurationException {
        Config c1 = new Config();
        c1.setActionTypes(Collections.singletonList(actionTypes.get(0)));

        Config c2 = new Config();
        c2.setActionTypes(Collections.singletonList(actionTypes.get(1)));

        actionTypes.get(0).setXmlns("fake");

        exception.expect(ConfigurationException.class);
        ConfigurationMerger.mergeConfiguration(c1, c2);
    }

    @Test
    public void testMismatchedTags() throws ConfigurationException {
        Config c1 = new Config();
        c1.setActionTypes(Collections.singletonList(actionTypes.get(0)));

        Config c2 = new Config();
        c2.setActionTypes(Collections.singletonList(actionTypes.get(1)));

        actionTypes.get(0).setTag("fake");

        exception.expect(ConfigurationException.class);
        ConfigurationMerger.mergeConfiguration(c1, c2);
    }

    @Test
    public void testMergeConfiguration() throws ConfigurationException {
        Config c1 = new Config();
        c1.setActionTypes(Collections.singletonList(actionTypes.get(0)));
        c1.setKillMessage("message");

        Config c2 = new Config();
        c2.setActionTypes(Collections.singletonList(actionTypes.get(1)));
        c2.setKillName("name");

        Config expected = new Config();
        expected.setKillName("name");
        expected.setKillMessage("message");
        ActionType merged = new ActionType();
        expected.setActionTypes(Collections.singletonList(merged));
        merged.setName(NAME);
        merged.setTag(TAG);
        merged.setXmlns(XLMNS);
        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("p1", "v1");
        expectedProperties.put("p2", "v2");
        expectedProperties.put("p3", "v3");
        merged.setProperties(expectedProperties);
        Map<String, List<String>> expectedArgs = new HashMap<>();
        expectedArgs.put("a", Lists.newArrayList("d", "c", "a", "b", "c"));
        merged.setDefaultArgs(expectedArgs);

        assertEquals(expected, ConfigurationMerger.mergeConfiguration(c1, c2));
    }

    @Test
    public void testGetFirstNonNullValue() {
        actionTypes.get(0).setName(null);

        assertEquals(actionTypes.get(1).getName(), ConfigurationMerger.getFirstNonNull(actionTypes, new Function<ActionType, String>() {
            @Override
            public String apply(ActionType input) {
                return input.getName();
            }
        }));
    }

    @Test
    public void testMergeMaps() {
        Map<String, String> expected = new HashMap<>();
        expected.put("p1", "v1");
        expected.put("p2", "v1");
        expected.put("p3", "v3");

        assertEquals(expected, ConfigurationMerger.mergeMaps(actionTypes, new Function<ActionType, Map<String, String>>() {
            @Override
            public Map<String, String> apply(ActionType input) {
                return input.getProperties();
            }
        }));
    }

    @Test
    public void testMergeCollectionMaps() {
        Map<String, List<String>> expected = new HashMap<>();
        expected.put("a", Lists.newArrayList("a", "b", "c", "d", "c"));

        assertEquals(expected, ConfigurationMerger.mergeCollectionMaps(actionTypes, new Function<ActionType, Map<String, List<String>>>() {
            @Override
            public Map<String, List<String>> apply(ActionType input) {
                return input.getDefaultArgs();
            }
        }));
    }

    @Test
    public void testAreAllValuesEqual() {
        assertFalse(ConfigurationMerger.areAllValuesEqual(actionTypes, new Function<ActionType, Boolean>() {
            @Override
            public Boolean apply(ActionType input) {
                return input.isLowPrecedence();
            }
        }));

        assertTrue(ConfigurationMerger.areAllValuesEqual(actionTypes, new Function<ActionType, String>() {
            @Override
            public String apply(ActionType input) {
                return input.getName();
            }
        }));
    }
}