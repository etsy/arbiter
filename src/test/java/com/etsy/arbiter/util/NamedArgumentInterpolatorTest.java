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

package com.etsy.arbiter.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class NamedArgumentInterpolatorTest {
    private Map<String, String> namedArgs;

    @Before
    public void setup() {
        namedArgs = new HashMap<>();
        namedArgs.put("key", "value");
    }

    @Test
    public void testNullNamedArgs() {
        Map<String, List<String>> args = new HashMap<>();
        args.put("one", Arrays.asList("two", "three"));

        Map<String, List<String>> result = NamedArgumentInterpolator.interpolate(args, null);
        assertTrue(result == args);
    }

    @Test
    public void testReferenceEquality() {
        Map<String, List<String>> args = new HashMap<>();
        args.put("one", Arrays.asList("two", "three"));

        Map<String, List<String>> result = NamedArgumentInterpolator.interpolate(args, namedArgs);

        assertFalse(args == result);
        assertEquals(args, result);
        assertFalse(args.get("one") == result.get("one"));
    }

    @Test
    public void testInterpolation() {
        Map<String, List<String>> args = new HashMap<>();
        args.put("one", Arrays.asList("$$key$$", "three"));

        Map<String, List<String>> result = NamedArgumentInterpolator.interpolate(args, namedArgs);
        Map<String, List<String>> expected = new HashMap<>();
        expected.put("one", Arrays.asList("value", "three"));

        assertEquals(expected, result);
    }

    @Test
    public void testSingleStringInterpolation() {
        String input = "hello $$key$$";
        String expected = "hello value";

        assertEquals(expected, NamedArgumentInterpolator.interpolate(input, namedArgs));

    }
}