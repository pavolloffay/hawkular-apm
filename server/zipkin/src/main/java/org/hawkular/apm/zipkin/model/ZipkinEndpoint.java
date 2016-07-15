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

package org.hawkular.apm.zipkin.model;

/**
 * @author Pavol Loffay
 */
public class ZipkinEndpoint {

    private String serviceName;
    private int ipv4;
    private Short port;


    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public int getIpv4() {
        return ipv4;
    }

    public void setIpv4(int ipv4) {
        this.ipv4 = ipv4;
    }

    public Short getPort() {
        return port;
    }

    public void setPort(Short port) {
        this.port = port;
    }
}
