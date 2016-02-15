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

package com.etsy.arbiter.workflow;

import com.etsy.arbiter.Action;
import com.etsy.arbiter.Workflow;
import com.etsy.arbiter.config.Config;
import com.etsy.arbiter.exception.WorkflowGraphException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.WorkflowEdge;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class WorkflowGraphBuilderTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public Workflow workflow;
    public Config config;

    @Before
    public void setup() {
        workflow = new Workflow();
        workflow.setName("workflow");
        Action a1 = new Action();
        a1.setName("a1");
        Action a2 = new Action();
        a2.setName("a2");
        Action a3 = new Action();
        a3.setName("a3");

        workflow.setActions(Arrays.asList(a1, a2, a3));

        config = new Config();
        config.setKillName("kill");
        config.setKillMessage("kill");
    }

    @Test
    public void testMissingDependency() throws WorkflowGraphException {
        workflow.getActions().get(0).setDependencies(Sets.newHashSet("missing"));
        expectedException.expect(WorkflowGraphException.class);
        WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
    }

    @Test
    public void testCycles() throws WorkflowGraphException {
        workflow.getActions().get(0).setDependencies(Sets.newHashSet("a2"));
        workflow.getActions().get(1).setDependencies(Sets.newHashSet("a1"));
        expectedException.expect(WorkflowGraphException.class);
        WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
    }

    @Test
    public void testSelfEdge() throws WorkflowGraphException {
        workflow.getActions().get(0).setDependencies(Sets.newHashSet("a1"));
        expectedException.expect(IllegalArgumentException.class);
        WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
    }

    @Test
    public void testNoDependencies() throws WorkflowGraphException {
        DirectedAcyclicGraph<Action, WorkflowEdge> graph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
        Set<String> expectedVertices = Sets.newHashSet("a1", "a2", "a3", "start", "end", "kill", "fork-0", "join-0");
        Set<String> expectedEdges = Sets.newHashSet("a3:join-0", "join-0:end", "a1:join-0", "fork-0:a2", "fork-0:a1", "a2:join-0", "fork-0:a3", "start:fork-0");
        assertEquals(expectedVertices, getVertices(graph));
        assertEquals(expectedEdges, getEdges(graph));
    }

    @Test
    public void testTwoActionFork() throws WorkflowGraphException {
        workflow.getActions().get(2).setDependencies(Sets.newHashSet("a1", "a2"));
        DirectedAcyclicGraph<Action, WorkflowEdge> graph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
        Set<String> expectedVertices = Sets.newHashSet("a1", "a2", "a3", "start", "end", "kill", "fork-0", "join-0");
        Set<String> expectedEdges = Sets.newHashSet("start:fork-0", "fork-0:a1", "fork-0:a2", "a1:join-0", "a2:join-0", "join-0:a3", "a3:end");
        assertEquals(expectedVertices, getVertices(graph));
        assertEquals(expectedEdges, getEdges(graph));
    }

    @Test
    public void testSingleStartNode() throws WorkflowGraphException {
        workflow.getActions().get(1).setDependencies(Sets.newHashSet("a1"));
        workflow.getActions().get(2).setDependencies(Sets.newHashSet("a1"));
        DirectedAcyclicGraph<Action, WorkflowEdge> graph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
        Set<String> expectedVertices = Sets.newHashSet("a1", "a2", "a3", "start", "end", "kill", "fork-0", "join-0");
        Set<String> expectedEdges = Sets.newHashSet("fork-0:a2", "a3:join-0", "fork-0:a3", "start:a1", "a2:join-0", "join-0:end", "a1:fork-0");
        assertEquals(expectedVertices, getVertices(graph));
        assertEquals(expectedEdges, getEdges(graph));
    }

    @Test
    public void testChain() throws WorkflowGraphException {
        workflow.getActions().get(1).setDependencies(Sets.newHashSet("a1"));
        workflow.getActions().get(2).setDependencies(Sets.newHashSet("a2"));
        DirectedAcyclicGraph<Action, WorkflowEdge> graph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
        Set<String> expectedVertices = Sets.newHashSet("a1", "a2", "a3", "start", "end", "kill");
        Set<String> expectedEdges = Sets.newHashSet("start:a1", "a1:a2", "a2:a3", "a3:end");
        assertEquals(expectedVertices, getVertices(graph));
        assertEquals(expectedEdges, getEdges(graph));
    }

    @Test
    public void testDisconnectedComponents() throws WorkflowGraphException {
        workflow.getActions().get(2).setDependencies(Sets.newHashSet("a2"));
        DirectedAcyclicGraph<Action, WorkflowEdge> graph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, null, false, null);
        Set<String> expectedVertices = Sets.newHashSet("a1", "a2", "a3", "start", "end", "kill", "fork-0", "join-0");
        Set<String> expectedEdges = Sets.newHashSet("a3:join-0", "a2:a3", "join-0:end", "a1:join-0", "fork-0:a2", "fork-0:a1", "start:fork-0");
        assertEquals(expectedVertices, getVertices(graph));
        assertEquals(expectedEdges, getEdges(graph));
    }

    private Set<String> getVertices(final DirectedAcyclicGraph<Action, WorkflowEdge> graph) {
        return Sets.newHashSet(Collections2.transform(graph.vertexSet(), new Function<Action, String>() {
            @Override
            public String apply(Action input) {
                return input.getName();
            }
        }));
    }

    private Set<String> getEdges(final DirectedAcyclicGraph<Action, WorkflowEdge> graph) {
        return Sets.newHashSet(Collections2.transform(graph.edgeSet(), new Function<WorkflowEdge, String>() {
            @Override
            public String apply(WorkflowEdge input) {
                return String.format("%s:%s", graph.getEdgeSource(input).getName(), graph.getEdgeTarget(input).getName());
            }
        }));
    }
}
