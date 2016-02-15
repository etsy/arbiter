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

package com.etsy.arbiter.util;

import com.etsy.arbiter.Action;
import org.apache.log4j.Logger;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.WorkflowEdge;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * Generates a Graphviz DOT file and PNG from a workflow graph
 *
 * @author Andrew Johnson
 */
public class GraphvizGenerator {
    private GraphvizGenerator() { }

    private static final Logger LOG = Logger.getLogger(GraphvizGenerator.class);

    /**
     * Generate a Graphviz DOT file and PNG from a workflow graph
     *
     * @param graph The graph from which to generate the Graphviz file
     * @param fileName The name of the DOT file to be generated
     * @param graphvizFormat The format in which Graphviz graphs should be generated if enabled
     */
    public static void generateGraphviz(DirectedAcyclicGraph<Action, WorkflowEdge> graph, String fileName, String graphvizFormat) {
        DOTExporter<Action, WorkflowEdge> exporter = new DOTExporter<>(new VertexNameProvider<Action>() {
            @Override
            public String getVertexName(Action o) {
                // This must produce a unique id for each vertex
                // This is never displayed, however
                return o.hashCode() + "";
            }
        }, new VertexNameProvider<Action>() {
            @Override
            public String getVertexName(Action o) {
                // This is the value displayed in the vertices of the generated graph
                return o.getName();
            }
        }, new EdgeNameProvider<WorkflowEdge>() {
            @Override
            public String getEdgeName(WorkflowEdge WorkflowEdge) {
                // This is the edge label
                // We don't need to label any edges in the graph
                return "";
            }
        });
        try {
            FileWriter writer = new FileWriter(fileName);
            exporter.export(writer, graph);
            writer.close();

            List<String> dotShellCommand = Arrays.asList("dot", String.format("-T%s", graphvizFormat), fileName, String.format("-O%s",
                    fileName.replace(".dot", String.format(".%s", graphvizFormat))));
            ProcessBuilder processBuilder = new ProcessBuilder(dotShellCommand)
                    .redirectErrorStream(true)
                    .directory(new File("."));
            Process p = processBuilder.start();

            BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

            int result = p.waitFor();
            if (result != 0) {
                LOG.warn(String.format("dot command exited unsuccessfully with exit code %d", result));
            }
        } catch (IOException | InterruptedException e) {
            LOG.warn("Error generating Graphviz", e);
        }
    }
}
