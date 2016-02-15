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

package com.etsy.arbiter;

import com.etsy.arbiter.util.YamlReader;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ParserTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private YamlReader<Workflow> parser;

    @Before
    public void setup() {
        parser = new YamlReader<>(Workflow.getYamlConstructor());
    }

    @Test
    public void testRead() {
        Workflow actual = parser.read(getClass().getClassLoader().getResource("testworkflow.yaml"));
        
        Workflow expected = new Workflow();
        expected.setName("name");
        Action action1 = new Action();
        Action action2 = new Action();
        expected.setActions(Arrays.asList(action1, action2));

        action1.setName("action1");
        action1.setType("test");
        Map<String, List<String>> args = new HashMap<>();
        args.put("one", Arrays.asList("two", "three"));
        action1.setPositionalArgs(args);

        action2.setName("action2");
        action2.setType("test");
        action2.setDependencies(Sets.newHashSet("action1"));
        args = new HashMap<>();
        args.put("two", Arrays.asList("four", "six"));
        Map<String, String> namedArgs = new HashMap<>();
        namedArgs.put("nameArg", "value");
        action2.setNamedArgs(namedArgs);
        action2.setPositionalArgs(args);

        Action error = new Action();
        error.setName("error");
        error.setType("errorTest");
        args = new HashMap<>();
        args.put("e", Arrays.asList("f", "g"));
        error.setPositionalArgs(args);
        expected.setErrorHandler(error);

        assertEquals(expected, actual);
    }

    @Test
    public void testReadWithConditionalDependencies() {
        Workflow actual = parser.read(getClass().getClassLoader().getResource("decision_node_workflow.yaml"));

        Workflow expected = new Workflow();
        expected.setName("name");
        Action action1 = new Action();
        Action action2 = new Action();
        expected.setActions(Arrays.asList(action1, action2));

        action1.setName("action1");
        action1.setType("test");
        Map<String, List<String>> args = new HashMap<>();
        args.put("one", Arrays.asList("two", "three"));
        action1.setPositionalArgs(args);

        action2.setName("action2");
        action2.setType("test");
        Map<String, String> conditionalDependencies = new HashMap<>();
        conditionalDependencies.put("action1", "${test:elFunction()}");
        action2.setConditionalDependencies(conditionalDependencies);
        args = new HashMap<>();
        args.put("two", Arrays.asList("four", "six"));
        Map<String, String> namedArgs = new HashMap<>();
        namedArgs.put("nameArg", "value");
        action2.setNamedArgs(namedArgs);
        action2.setPositionalArgs(args);

        Action error = new Action();
        error.setName("error");
        error.setType("errorTest");
        args = new HashMap<>();
        args.put("e", Arrays.asList("f", "g"));
        error.setPositionalArgs(args);
        expected.setErrorHandler(error);

        assertEquals(expected, actual);
    }

    @Test
    public void testReadNonExistentFile() {
        exception.expect(RuntimeException.class);
        parser.read(new File("nonexistent.yaml"));
    }

    @Test
    public void testReadNonExistentResource() {
        exception.expect(NullPointerException.class);
        parser.read(getClass().getClassLoader().getResource("nonexistent.yaml"));
    }
}