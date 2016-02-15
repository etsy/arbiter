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

package org.jgrapht.graph;

/**
 * Represents an edge between nodes in a workflow
 * Optionally a condition may be specified, indicating that a decision node is needed
 */
public class WorkflowEdge extends DefaultEdge {
    private String condition;

    /**
     * Create a WorkflowEdge with no condition
     * This is the standard behavior
     */
    public WorkflowEdge() {
        this.condition = null;
    }

    public WorkflowEdge(String condition) {
        this.condition = condition;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String toString() {
        return "(" + this.source + " : " + this.target + " (condition: " + condition + ")";
    }
}
