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

import com.etsy.arbiter.config.ActionType;
import com.etsy.arbiter.config.Config;
import com.etsy.arbiter.exception.WorkflowGraphException;
import com.etsy.arbiter.util.GraphvizGenerator;
import com.etsy.arbiter.util.NamedArgumentInterpolator;
import com.etsy.arbiter.workflow.WorkflowGraphBuilder;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.WorkflowEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.w3c.dom.Document;
import org.xembly.Directives;
import org.xembly.ImpossibleModificationException;
import org.xembly.Xembler;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Generates Oozie workflows from Arbiter workflows
 *
 * @author Andrew Johnson
 */
public class OozieWorkflowGenerator {
    private static final Logger LOG = Logger.getLogger(OozieWorkflowGenerator.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    private Config config;
    private Map<String, ActionType> actionTypeCache;

    public OozieWorkflowGenerator(Config config) {
        this.config = config;
        actionTypeCache = new HashMap<>();
    }

    /**
     * Generate Oozie workflows from Arbiter workflows
     *
     * @param outputBase The directory in which to output the Oozie workflows
     * @param workflows The workflows to convert
     * @param generateGraphviz Indicate if Graphviz graphs should be generated for workflows
     * @param graphvizFormat The format in which Graphviz graphs should be generated if enabled
     */
    public void generateOozieWorkflows(String outputBase, List<Workflow> workflows, boolean generateGraphviz, String graphvizFormat) throws IOException, ParserConfigurationException, TransformerException {
        File outputBaseFile = new File(outputBase);
        FileUtils.forceMkdir(outputBaseFile);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        Date currentDate = new Date();
        String currentDateString = DATE_FORMAT.format(currentDate);

        for (Workflow workflow : workflows) {
            String outputDir = outputBase + "/" + workflow.getName();
            File outputDirFile = new File(outputDir);
            FileUtils.forceMkdir(outputDirFile);
            DirectedAcyclicGraph<Action, WorkflowEdge> workflowGraph = null;

            try {
                workflowGraph = WorkflowGraphBuilder.buildWorkflowGraph(workflow, config, outputDir, generateGraphviz, graphvizFormat);
            } catch (WorkflowGraphException w) {
                LOG.error("Unable to generate workflow", w);
                System.exit(1);
            }

            if (generateGraphviz) {
                GraphvizGenerator.generateGraphviz(workflowGraph, outputDir + "/" + workflow.getName() + ".dot", graphvizFormat);
            }

            Document xmlDoc = builder.newDocument();

            Directives directives = new Directives();
            createRootElement(workflow.getName(), directives);

            Action kill = getActionByType(workflowGraph, "kill");
            Action end = getActionByType(workflowGraph, "end");
            Action start = getActionByType(workflowGraph, "start");
            Action errorHandler = workflow.getErrorHandler();
            Action finalTransition = kill == null ? end : kill;

            Action errorTransition = errorHandler == null ? (kill == null ? end : kill) : errorHandler;
            DepthFirstIterator<Action, WorkflowEdge> iterator = new DepthFirstIterator<>(workflowGraph, start);

            while (iterator.hasNext()) {
                Action a = iterator.next();
                Action transition = getTransition(workflowGraph, a);
                switch (a.getType()) {
                    case "start":
                        if (transition == null) {
                            throw new RuntimeException("No transition found for start action");
                        }
                        directives.add("start")
                                .attr("to", transition.getName())
                                .up();
                        break;
                    case "end":
                        // Skip and add at the end
                        break;
                    case "fork":
                        directives.add("fork")
                                .attr("name", a.getName());
                        for (WorkflowEdge edge : workflowGraph.outgoingEdgesOf(a)) {
                            Action target = workflowGraph.getEdgeTarget(edge);
                            directives.add("path")
                                    .attr("start", target.getName())
                                    .up();
                        }
                        directives.up();
                        break;
                    case "join":
                        if (transition == null) {
                            throw new RuntimeException(String.format("No transition found for join action %s", a.getName()));
                        }
                        directives.add("join")
                                .attr("name", a.getName())
                                .attr("to", transition.getName())
                                .up();
                        break;
                    default:
                        createActionElement(a, workflowGraph, transition, a.equals(errorHandler) ? finalTransition : errorTransition, directives);
                        directives.up();
                        break;
                }
            }
            if (kill != null) {
                directives.add("kill")
                        .attr("name", kill.getName())
                        .add("message")
                        .set(kill.getNamedArgs().get("message"))
                        .up()
                        .up();
            }
            if (end != null) {
                directives.add("end")
                        .attr("name", end.getName())
                        .up();
            }

            try {
                new Xembler(directives).apply(xmlDoc);
            } catch (ImpossibleModificationException e) {
                throw new RuntimeException(e);
            }
            writeDocument(outputDirFile, xmlDoc, transformer, workflow.getName(), currentDateString);
        }
    }

    /**
     * Add the XML element for an action
     *
     * @param action The action for which to add the element
     * @param workflowGraph The full workflow graph
     * @param transition The OK transition for this action
     * @param errorTransition The error transition for this action if it is not inside a fork/join pair
     * @param directives The Xembly Directives object to which to add the new XML elements
     */
    private void createActionElement(Action action, DirectedAcyclicGraph<Action, WorkflowEdge> workflowGraph, Action transition, Action errorTransition, Directives directives) {
        ActionType type = getActionType(action.getType());

        directives.add("action")
                .attr("name", action.getName())
                .add(type.getTag());

        if (type.getXmlns() != null) {
            directives.attr("xmlns", type.getXmlns());
        }

        // There is an outer action tag and an inner tag corresponding to the action type
        Map<String, List<String>> interpolated = NamedArgumentInterpolator.interpolate(type.getDefaultArgs(), action.getNamedArgs(), type.getDefaultInterpolations(), action.getPositionalArgs());
        Map<String, String> mergedConfigurationProperties = new HashMap<>(type.getProperties());
        if (action.getConfigurationProperties() != null) {
            mergedConfigurationProperties.putAll(action.getConfigurationProperties());
        }
        addInnerActionElements(mergedConfigurationProperties, type.getConfigurationPosition(), directives, interpolated, action.getPositionalArgs());
        directives.up();

        String okTransitionName = action.getForceOk() != null ? action.getForceOk() : transition.getName();
        directives.add("ok")
                .attr("to", okTransitionName)
                .up();

        // We allow forcing a particular error transition regardless of other considerations
        String interpolatedForceError = NamedArgumentInterpolator.interpolate(action.getForceError(), ImmutableMap.of("okTransition", okTransitionName), type.getDefaultInterpolations());
        String errorTransitionName = interpolatedForceError != null ? interpolatedForceError : errorTransition.getName();
        // Find the enclosing fork/join pair
        // If an action is inside a fork/join, it should transition to the join on error
        String enclosingJoinName = getEnclosingForkJoinName(action, workflowGraph);
        if (enclosingJoinName != null) {
            errorTransitionName = interpolatedForceError != null ? interpolatedForceError : enclosingJoinName;
        }
        directives.add("error")
                .attr("to", errorTransitionName)
                .up();
    }

    /**
     * Finds the enclosing fork/join pair for an action
     * This is used to set the error transition for nodes inside a fork/join
     *
     * @param action The action for which to find the enclosing fork/join pair
     * @param workflowGraph The graph in which to find the enclosing fork/join pair
     * @return The name of the join for the fork/join pair enclosing the given action, or null if the action is not inside a fork/join
     */
    private String getEnclosingForkJoinName(Action action, DirectedAcyclicGraph<Action, WorkflowEdge> workflowGraph) {
        List<String> forks = new ArrayList<>();
        List<String> joins = new ArrayList<>();

        // If the action has no incoming or outgoing edges, it is definitely not inside a fork/join
        if (workflowGraph.inDegreeOf(action) == 0) {
            return null;
        }

        if (workflowGraph.outDegreeOf(action) == 0) {
            return null;
        }

        // First we traverse backwards from the given action, recording all the forks we see
        Action curr = action;
        while (workflowGraph.inDegreeOf(curr) > 0) {
            WorkflowEdge incoming = Lists.newArrayList(workflowGraph.incomingEdgesOf(curr)).get(0);
            curr = workflowGraph.getEdgeSource(incoming);
            if (curr.getType().equals("fork")) {
                forks.add(curr.getName());
            }
        }

        // Then we traverse forwards from the given action, recording all the joins we see
        curr = action;
        while (workflowGraph.outDegreeOf(curr) > 0) {
            WorkflowEdge outgoing = Lists.newArrayList(workflowGraph.outgoingEdgesOf(curr)).get(0);
            curr = workflowGraph.getEdgeTarget(outgoing);
            if (curr.getType().equals("join")) {
                joins.add(curr.getName());
            }
        }

        // At this point we have a list of all the forks before to the given action and all the joins after the given action
        // The first fork in this list where the corresponding join was also seen is the correct enclosing fork/join
        for (String fork : forks) {
            String joinName = fork.replace("fork", "join");
            if (joins.contains(joinName)) {
                return joinName;
            }
        }

        // If there are no matching fork/join pairs, then this action is not inside a fork/join pair
        return null;
    }

    /**
     * Add elements to the inner action tag (e.g. java as opposed to the outer action tag)
     *
     * @param properties The configuration properties for this action
     * @param configurationPosition The position within the tag where the configuration should be placed
     * @param directives The Xembly Directives object to which to add the new XML elements
     * @param interpolated Interpolated arguments from the YAML workflow definition
     * @param positional Positional arguments from the YAML workflow definition
     */
    private void addInnerActionElements(Map<String, String> properties, int configurationPosition, Directives directives, Map<String, List<String>> interpolated, Map<String, List<String>> positional) {
        List<Map.Entry<String, List<String>>> entries = new ArrayList<>();
        if (interpolated != null) {
            entries.addAll(interpolated.entrySet());
        }
        if (positional != null) {
            entries.addAll(positional.entrySet());
        }

        for (int i = 0; i < entries.size(); i++) {
            if (configurationPosition == i) {
                createConfigurationElement(properties, directives);
            }
            addKeyMultiValueElements(entries.get(i), directives);
        }

        if (entries.size() < configurationPosition) {
            createConfigurationElement(properties, directives);
        }
    }

    /**
     * Add the configuration element for a workflow action
     *
     * @param properties The configuration properties
     * @param directives The Xembly Directives object to which to add the new XML elements
     */
    private void createConfigurationElement(Map<String, String> properties, Directives directives) {
        if (properties == null) {
            return;
        }

        directives.add("configuration");

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            directives.add("property")
                    .add("name")
                    .set(entry.getKey())
                    .up()
                    .add("value")
                    .set(entry.getValue())
                    .up()
                    .up();
        }
        directives.up();
    }

    /**
     * Add the elements where a given key has multiple values
     * An example of this is the arg tag
     *
     * @param entry A mapping of key to a list of values
     * @param directives The Xembly Directives object to which to add the new XML elements
     */
    private void addKeyMultiValueElements(Map.Entry<String, List<String>> entry, Directives directives) {
        for (String value : entry.getValue()) {
            directives.add(entry.getKey())
                    .set(value)
                    .up();
        }
    }

    /**
     * Get the ActionType configuration from a type name
     * This caches the results to avoid searching the whole configuration repeatedly
     *
     * @param type The name of the action type for which to retrieve the configuration
     * @return An ActionType object representing the configuration for the given action type
     */
    private ActionType getActionType(String type) {
        ActionType result = actionTypeCache.get(type);
        if (result == null) {
            result = config.getActionTypeByName(type);
            actionTypeCache.put(type, result);
        }

        return result;
    }

    /**
     * Write an XML document to a file
     *
     * @param outputDir The output directory for the XML file. This directory must exist before this method is called
     * @param xmlDoc The document to write out
     * @param transformer The XML transformer used to produce the output
     * @param name The name of the workflow
     * @param currentDateString A string representation of the current date, used in a comment in the output file
     * @throws TransformerException
     * @throws IOException
     */
    private void writeDocument(File outputDir, Document xmlDoc, Transformer transformer, String name, String currentDateString) throws TransformerException, IOException {
        DOMSource source = new DOMSource(xmlDoc);
        File outputFile = new File(outputDir, "workflow.xml");
        StreamResult result = new StreamResult(outputFile);
        transformer.transform(source, result);

        // We want a comment indicating that this file is autogenerated as the first line
        // There's no good way to do this from the XML DOM, so we have to do it manually.
        BufferedReader reader = new BufferedReader(new FileReader(outputFile));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        reader.close();

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        for (int i = 0; i < lines.size(); i++) {
            line = lines.get(i);
            // Comments have to appear after the <?xml line
            if (i == 1) {
                writer.write(String.format("<!-- %s workflow autogenerated by Arbiter on %s -->\n", name, currentDateString));
            }
            writer.write(line + "\n");
        }
        writer.close();
    }

    /**
     * Create the root XML element
     *
     * @param name The name of the root element
     * @param directives The Xembly Directives object to which to add the root element
     */
    private void createRootElement(String name, Directives directives) {
        directives.add("workflow-app")
                .attr("xmlns", "uri:oozie:workflow:0.2")
                .attr("name", name);
    }

    /**
     * Gets the OK transition for an action
     *
     * @param workflowGraph The graph from which to get the transition
     * @param a The action for which to get the transition
     * @return The OK transition for the given action
     */
    private Action getTransition(DirectedAcyclicGraph<Action, WorkflowEdge> workflowGraph, Action a) {
        Set<WorkflowEdge> transitions = workflowGraph.outgoingEdgesOf(a);
        // end and kill nodes do not transition at all
        // forks have multiple transitions and are handled specially
        if (a.getType().equals("end") || a.getType().equals("kill") || a.getType().equals("fork")) {
            return null;
        }
        // This would be a very odd case, as only forks can have multiple transitions
        // This should be impossible, but just in case we catch it and throw an exception
        if (transitions.size() != 1)  {
            throw new RuntimeException("Multiple transitions found for action " + a.getName());
        }

        WorkflowEdge transition = transitions.iterator().next();
        return workflowGraph.getEdgeTarget(transition);
    }

    /**
     * Finds the first action in the graph of a given type
     * This is intended for use when only one action of the given type exists
     * If more than one exists the traversal order and thus the resulting action is not guaranteed
     *
     * @param workflowGraph The graph in which to find the action
     * @param type The type of action to find
     * @return The action of the given type, or null if none exists
     */
    private Action getActionByType(DirectedAcyclicGraph<Action, WorkflowEdge> workflowGraph, final String type) {
        List<Action> actionList = Lists.newArrayList(Collections2.filter(workflowGraph.vertexSet(), new Predicate<Action>() {
            @Override
            public boolean apply(Action input) {
                return input.getType().equals(type);
            }
        }));
        if (actionList.size() > 0) {
            return actionList.get(0);
        } else {
            return null;
        }
    }
}
