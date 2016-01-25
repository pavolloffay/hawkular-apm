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
package org.hawkular.btm.api.internal.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.hawkular.btm.api.model.btxn.ProcessorIssue;
import org.hawkular.btm.api.model.config.Direction;
import org.hawkular.btm.api.model.config.btxn.LiteralExpression;
import org.hawkular.btm.api.model.config.btxn.Processor;
import org.hawkular.btm.api.model.config.btxn.SetPropertyAction;
import org.junit.Test;

/**
 * @author gbrown
 */
public class LiteralExpressionHandlerTest {

    @Test
    public void testPredicateTrue() {
        LiteralExpression literal = new LiteralExpression();
        literal.setValue("true");

        LiteralExpressionHandler handler = new LiteralExpressionHandler(literal);
        handler.init(null, null, true);

        assertTrue(handler.test(null, null, Direction.In, null, null));
        assertNull(handler.getIssues());
    }

    @Test
    public void testPredicateFalse() {
        LiteralExpression literal = new LiteralExpression();
        literal.setValue("False");

        LiteralExpressionHandler handler = new LiteralExpressionHandler(literal);
        handler.init(null, null, true);

        assertFalse(handler.test(null, null, Direction.In, null, null));
        assertNull(handler.getIssues());
    }

    @Test
    public void testPredicateInvalid() {
        LiteralExpression literal = new LiteralExpression();
        literal.setValue("invalid");

        Processor processor = new Processor();
        processor.setDescription("Processor Description");

        SetPropertyAction action = new SetPropertyAction();
        action.setDescription("Action Description");

        LiteralExpressionHandler handler = new LiteralExpressionHandler(literal);
        handler.init(processor, action, true);

        assertFalse(handler.test(null, null, Direction.In, null, null));

        assertEquals(1, handler.getIssues().size());
        assertEquals(processor.getDescription(), ((ProcessorIssue) handler.getIssues().get(0)).getProcessor());
        assertEquals(action.getDescription(), ((ProcessorIssue) handler.getIssues().get(0)).getAction());
    }

    @Test
    public void testEvaluateValue() {
        LiteralExpression literal = new LiteralExpression();
        literal.setValue("hello");

        LiteralExpressionHandler handler = new LiteralExpressionHandler(literal);
        handler.init(null, null, false);

        assertEquals(literal.getValue(), handler.evaluate(null, null, Direction.In, null, null));
        assertNull(handler.getIssues());
    }

}
