/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.btm.client.collector.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.hawkular.btm.api.internal.actions.ExpressionHandler;
import org.hawkular.btm.api.internal.actions.ExpressionHandlerFactory;
import org.hawkular.btm.api.internal.actions.ProcessorActionHandler;
import org.hawkular.btm.api.internal.actions.ProcessorActionHandlerFactory;
import org.hawkular.btm.api.logging.Logger;
import org.hawkular.btm.api.logging.Logger.Level;
import org.hawkular.btm.api.model.Severity;
import org.hawkular.btm.api.model.btxn.BusinessTransaction;
import org.hawkular.btm.api.model.btxn.Component;
import org.hawkular.btm.api.model.btxn.Issue;
import org.hawkular.btm.api.model.btxn.Node;
import org.hawkular.btm.api.model.btxn.NodeType;
import org.hawkular.btm.api.model.btxn.ProcessorIssue;
import org.hawkular.btm.api.model.config.CollectorConfiguration;
import org.hawkular.btm.api.model.config.Direction;
import org.hawkular.btm.api.model.config.btxn.BusinessTxnConfig;
import org.hawkular.btm.api.model.config.btxn.Processor;
import org.hawkular.btm.api.model.config.btxn.ProcessorAction;

/**
 * This class manages the processors.
 *
 * @author gbrown
 */
public class ProcessorManager {

    private static Logger log = Logger.getLogger(ProcessorManager.class.getName());

    private Map<String, List<ProcessorWrapper>> processors = new HashMap<String, List<ProcessorWrapper>>();

    /**
     * This constructor initialises the processor manager with the configuration.
     *
     * @param config The configuration
     */
    public ProcessorManager(CollectorConfiguration config) {
        init(config);
    }

    /**
     * This method initialises the filter manager.
     *
     * @param config The configuration
     */
    protected void init(CollectorConfiguration config) {
        for (String btxn : config.getBusinessTransactions().keySet()) {
            BusinessTxnConfig btc = config.getBusinessTransactions().get(btxn);
            init(btxn, btc);
        }
    }

    /**
     * This method initialises the processors associated with the supplied
     * business transaction configuration.
     *
     * @param btxn The business transaction name
     * @param btc The configuration
     */
    public void init(String btxn, BusinessTxnConfig btc) {
        if (log.isLoggable(Level.FINE)) {
            log.fine("ProcessManager: initialise btxn '" + btxn + "' config=" + btc
                    + " processors=" + btc.getProcessors().size());
        }

        if (btc.getProcessors() != null && !btc.getProcessors().isEmpty()) {
            List<ProcessorWrapper> procs = new ArrayList<ProcessorWrapper>();

            for (int i = 0; i < btc.getProcessors().size(); i++) {
                procs.add(new ProcessorWrapper(btc.getProcessors().get(i)));
            }

            synchronized (processors) {
                processors.put(btxn, procs);
            }
        } else {
            synchronized (processors) {
                processors.remove(btxn);
            }
        }
    }

    /**
     * This method removes the business transaction configuration.
     *
     * @param btxn The business transaction name
     */
    public void remove(String btxn) {
        synchronized (processors) {
            processors.remove(btxn);
        }
    }

    /**
     * This method determines whether the business transaction, for the supplied node
     * and in/out direction, will process available information.
     *
     * @param btxn The business transaction
     * @param node The current node
     * @param direction The direction
     * @return Whether processing instructions have been defined
     */
    public boolean isProcessed(BusinessTransaction btxn, Node node, Direction direction) {
        boolean ret = false;

        if (btxn.getName() != null) {
            List<ProcessorWrapper> procs = null;

            synchronized (processors) {
                procs = processors.get(btxn.getName());
            }

            if (procs != null) {
                for (int i = 0; !ret && i < procs.size(); i++) {
                    ret = procs.get(i).isProcessed(btxn, node, direction);
                }
            }
        }

        if (log.isLoggable(Level.FINEST)) {
            log.finest("ProcessManager: isProcessed btxn=" + btxn + " node=" + node
                    + " direction=" + direction + "? " + ret);
        }

        return ret;
    }

    /**
     * This method determines whether the business transaction, for the supplied node
     * and in/out direction, will process content information.
     *
     * @param btxn The business transaction
     * @param node The current node
     * @param direction The direction
     * @return Whether content processing instructions have been defined
     */
    public boolean isContentProcessed(BusinessTransaction btxn, Node node, Direction direction) {
        boolean ret = false;

        if (btxn.getName() != null) {
            List<ProcessorWrapper> procs = null;

            synchronized (processors) {
                procs = processors.get(btxn.getName());
            }

            if (procs != null) {
                for (int i = 0; !ret && i < procs.size(); i++) {
                    ret = procs.get(i).isProcessed(btxn, node, direction)
                            && procs.get(i).usesContent();
                }
            }
        }

        if (log.isLoggable(Level.FINEST)) {
            log.finest("ProcessManager: isContentProcessed btxn=" + btxn + " node=" + node
                    + " direction=" + direction + "? " + ret);
        }

        return ret;
    }

    /**
     * This method processes the supplied information against the configured processor
     * details for the business transaction.
     *
     * @param btxn The business transaction
     * @param node The node being processed
     * @param direction The direction
     * @param headers The headers
     * @param values The values
     */
    public void process(BusinessTransaction btxn, Node node, Direction direction,
            Map<String, ?> headers, Object... values) {

        if (log.isLoggable(Level.FINEST)) {
            log.finest("ProcessManager: process btxn=" + btxn + " node=" + node
                    + " direction=" + direction + " headers=" + headers + " values=" + values
                    + " : available processors=" + processors);
        }

        if (btxn.getName() != null) {
            List<ProcessorWrapper> procs = null;

            synchronized (processors) {
                procs = processors.get(btxn.getName());
            }

            if (log.isLoggable(Level.FINEST)) {
                log.finest("ProcessManager: btxn name=" + btxn.getName() + " processors=" + procs);
            }

            if (procs != null) {
                for (int i = 0; i < procs.size(); i++) {
                    procs.get(i).process(btxn, node, direction, headers, values);
                }
            }
        }
    }

    /**
     * @return the processors
     */
    protected Map<String, List<ProcessorWrapper>> getProcessors() {
        return processors;
    }

    /**
     * @param processors the processors to set
     */
    protected void setProcessors(Map<String, List<ProcessorWrapper>> processors) {
        this.processors = processors;
    }

    /**
     * This class provides the execution behaviour associated with the
     * information defined in the collector configuration processor
     * definition.
     *
     * @author gbrown
     */
    public class ProcessorWrapper {

        private Processor processor;

        private Predicate<String> uriFilter = null;

        private Predicate<String> faultFilter = null;

        private ExpressionHandler predicateHandler = null;

        private List<ProcessorActionWrapper> actions = new ArrayList<ProcessorActionWrapper>();

        private boolean usesHeaders = false;
        private boolean usesContent = false;

        private List<Issue> issues;

        /**
         * This constructor is initialised with the processor.
         *
         * @param processor The processor
         */
        public ProcessorWrapper(Processor processor) {
            this.processor = processor;

            init();
        }

        /**
         * This method initialises the processor.
         */
        protected void init() {
            if (processor.getUriFilter() != null) {
                uriFilter = Pattern.compile(processor.getUriFilter()).asPredicate();
            }

            if (processor.getFaultFilter() != null) {
                faultFilter = Pattern.compile(processor.getFaultFilter()).asPredicate();
            }

            try {
                if (processor.getPredicate() != null) {
                    predicateHandler = ExpressionHandlerFactory.getHandler(processor.getPredicate());

                    predicateHandler.init(getProcessor(), null, true);

                    // Check if headers referenced
                    usesHeaders = predicateHandler.isUsesHeaders();
                    usesContent = predicateHandler.isUsesContent();
                }
            } catch (Throwable t) {
                if (log.isLoggable(Level.FINE)) {
                    log.log(Level.FINE, "Failed to initialise processor predicate '"
                            + processor.getPredicate() + "'", t);
                }

                ProcessorIssue pi = new ProcessorIssue();
                pi.setProcessor(processor.getDescription());
                pi.setSeverity(Severity.Error);
                pi.setDescription(t.getMessage());

                if (issues == null) {
                    issues = new ArrayList<Issue>();
                }
                issues.add(pi);
            }

            for (int i = 0; i < processor.getActions().size(); i++) {
                ProcessorActionWrapper paw = new ProcessorActionWrapper(processor,
                        processor.getActions().get(i));
                if (!usesHeaders) {
                    usesHeaders = paw.isUsesHeaders();
                }
                if (!usesContent) {
                    usesContent = paw.isUsesContent();
                }
                actions.add(paw);
            }
        }

        /**
         * This method returns the processor.
         *
         * @return The processor
         */
        protected Processor getProcessor() {
            return processor;
        }

        /**
         * This method indicates whether the process action uses headers.
         *
         * @return Whether headers are used
         */
        public boolean usesHeaders() {
            return usesHeaders;
        }

        /**
         * This method indicates whether the process action uses content values.
         *
         * @return Whether content is used
         */
        public boolean usesContent() {
            return usesContent;
        }

        /**
         * This method checks that this processor matches the supplied business txn
         * name and node details.
         *
         * @param btxn The business transaction
         * @param node The node
         * @param direction The direction
         * @return Whether the supplied details would be processed by this processor
         */
        public boolean isProcessed(BusinessTransaction btxn, Node node, Direction direction) {
            boolean ret = false;

            if (processor.getNodeType() == node.getType()
                    && processor.getDirection() == direction) {

                // If URI filter regex expression defined, then verify whether
                // node URI matches
                if (uriFilter == null
                        || uriFilter.test(node.getUri())) {
                    ret = true;
                }
            }

            if (log.isLoggable(Level.FINEST)) {
                log.finest("ProcessManager/Processor: isProcessed btxn=" + btxn + " node=" + node
                        + " direction=" + direction + "? " + ret);
            }

            return ret;
        }

        /**
         * This method processes the supplied information to extract the relevant
         * details.
         *
         * @param btxn The business transaction
         * @param node The node
         * @param direction The direction
         * @param headers The optional headers
         * @param values The values
         */
        public void process(BusinessTransaction btxn, Node node, Direction direction,
                Map<String, ?> headers, Object[] values) {

            if (log.isLoggable(Level.FINEST)) {
                log.finest("ProcessManager/Processor: process btxn=" + btxn + " node=" + node
                        + " direction=" + direction + " headers=" + headers + " values=" + values);

                if (values != null) {
                    for (int i = 0; i < values.length; i++) {
                        log.finest("        [value " + i + "] = " + values[i]);
                    }
                }
            }

            if (processor.getNodeType() == node.getType()
                    && processor.getDirection() == direction) {

                // If URI filter regex expression defined, then verify whether
                // node URI matches
                if (uriFilter != null
                        && !uriFilter.test(node.getUri())) {
                    return;
                }

                // Check if operation has been specified, and node is Component
                if (processor.getOperation() != null && node.getType() == NodeType.Component
                        && !processor.getOperation().equals(((Component) node).getOperation())) {
                    return;
                }

                // If fault filter not defined, then node cannot have a fault
                if (faultFilter == null && node.getFault() != null) {
                    return;
                }

                // If fault filter regex expression defined, then verify whether
                // node fault string matches.
                if (faultFilter != null && (node.getFault() == null
                        || !faultFilter.test(node.getFault()))) {
                    return;
                }

                // Associate any issues created during initialisation with
                // the node
                if (issues != null) {
                    node.getIssues().addAll(issues);
                }

                if (predicateHandler != null) {
                    try {
                        if (!predicateHandler.test(btxn, node, direction, headers, values)) {
                            if (log.isLoggable(Level.FINEST)) {
                                log.finest("ProcessManager/Processor: process - predicate returned false");
                            }
                            return;
                        }
                    } catch (Throwable t) {
                        ProcessorIssue pi = new ProcessorIssue();
                        pi.setProcessor(processor.getDescription());
                        pi.setSeverity(Severity.Error);
                        pi.setDescription(t.getMessage());
                        node.getIssues().add(pi);

                        return;
                    }
                }

                for (int i = 0; i < actions.size(); i++) {
                    actions.get(i).process(btxn, node, direction, headers, values);
                }
            }
        }

        @Override
        public String toString() {
            return processor.toString();
        }
    }

    /**
     * This class provides the execution behaviour associated with the
     * information defined in the collector configuration processor
     * definition.
     *
     * @author gbrown
     */
    public class ProcessorActionWrapper {

        private ProcessorActionHandler handler;
        private String processorDescription;
        private String actionDescription;

        /**
         * This constructor is initialised with the processor action.
         *
         * @param processor The processor
         * @param action The processor action
         */
        public ProcessorActionWrapper(Processor processor, ProcessorAction action) {
            this.processorDescription = processor.getDescription();
            this.actionDescription = action.getDescription();
            handler = ProcessorActionHandlerFactory.getHandler(action);
            if (handler != null) {
                handler.init(processor);
            }
        }

        /**
         * @return the usesHeaders
         */
        public boolean isUsesHeaders() {
            return handler.isUsesHeaders();
        }

        /**
         * @return the usesContent
         */
        public boolean isUsesContent() {
            return handler.isUsesContent();
        }

        /**
         * This method processes the supplied information to extract the relevant
         * details.
         *
         * @param btxn The business transaction
         * @param node The node
         * @param direction The direction
         * @param headers The optional headers
         * @param values The values
         */
        public void process(BusinessTransaction btxn, Node node, Direction direction,
                Map<String, ?> headers, Object[] values) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest("ProcessManager/Processor/Action[" + handler.getAction()
                        + "]: process btxn=" + btxn + " node=" + node
                        + " direction=" + direction + " headers=" + headers + " values=" + values);

                if (values != null) {
                    for (int i = 0; i < values.length; i++) {
                        log.finest("        [value " + i + "] = " + values[i]);
                    }
                }
            }

            // If expressions don't use headers or content values, then just process each
            // time process action is called, otherwise determine if the headers or content
            // have been provided
            if (handler.isUsesHeaders() || handler.isUsesContent()) {
                // Check if headers supplied if expressions requires them
                if (handler.isUsesHeaders() && (headers == null || headers.isEmpty())) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("ProcessManager/Processor/Action[" + handler.getAction()
                                + "]: uses headers but not supplied");
                    }
                    return;
                }

                // Check if content values supplied if expressions requires them
                if (handler.isUsesContent() && (values == null || values.length == 0)) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("ProcessManager/Processor/Action[" + handler.getAction()
                                + "]: uses content values but not supplied");
                    }
                    return;
                }
            }

            try {
                handler.process(btxn, node, direction, headers, values);
            } catch (Throwable t) {
                ProcessorIssue pi = new ProcessorIssue();
                pi.setProcessor(processorDescription);
                pi.setAction(actionDescription);
                pi.setSeverity(Severity.Error);
                pi.setDescription(t.getMessage());
                node.getIssues().add(pi);
            }
        }
    }
}
