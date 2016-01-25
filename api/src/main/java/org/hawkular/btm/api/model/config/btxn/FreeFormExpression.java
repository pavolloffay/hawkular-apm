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
package org.hawkular.btm.api.model.config.btxn;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * This class represents any complex expression that needs to be
 * evaluated.
 *
 * @author gbrown
 */
public class FreeFormExpression extends Expression {

    @JsonInclude
    private String value;

    /**
     * The default constructor.
     */
    public FreeFormExpression() {
    }

    /**
     * The constructor initialising the expression.
     *
     * @param value The value
     */
    public FreeFormExpression(String value) {
        this.value = value;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "FreeForm [value=" + value + "]";
    }

}
