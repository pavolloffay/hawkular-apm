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

package org.hawkular.apm.server.jms.span;

import java.io.File;

import javax.annotation.Resource;
import javax.jms.ConnectionFactory;
import javax.jms.Topic;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Pavol Loffay
 */
@RunWith(Arquillian.class)
public class SpanStoreMDBTest {

    @Deployment
    public static WebArchive createDeployment() {
        File[] localPomDeps = Maven.resolver().loadPomFromFile("pom.xml")
                .importRuntimeDependencies().resolve().withTransitivity().asFile();

        JavaArchive[] infinispan = Maven.resolver().resolve("org.hawkular.apm:hawkular-apm-server-infinispan:" +
                System.getProperty("project.version"))
                .withTransitivity()
                .as(JavaArchive.class);
        JavaArchive[] elastic = Maven.resolver().resolve("org.hawkular.apm:hawkular-apm-server-elasticsearch:" +
                System.getProperty("project.version"))
                .withTransitivity()
                .as(JavaArchive.class);

        JavaArchive jar = ShrinkWrap.create(JavaArchive.class)
                .addPackage("org.hawkular.apm.server.jms")
                .addPackage("org.hawkular.apm.server.jms.log")
                .addPackage("org.hawkular.apm.server.jms.span")
                .addPackage("org.hawkular.apm.server.jms.trace")
                .addClass(SpanStoreMDB.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

        WebArchive war = ShrinkWrap.create(WebArchive.class)
                .addAsLibraries(localPomDeps)
                .addAsLibraries(infinispan)
                .addAsLibraries(elastic)
                .addAsLibraries(jar)
//                .addPackages(true, "org.hawkular.apm.server.jms")
//                .addPackage("org.hawkular.apm.server.jms")
//                .addPackage("org.hawkular.apm.server.jms.log")
//                .addPackage("org.hawkular.apm.server.jms.span")
//                .addPackage("org.hawkular.apm.server.jms.trace")
//                .addClass(SpanStoreMDB.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");

        return war;
    }

    @Resource(mappedName = "/Spans")
    Topic spanTopic;

    @Resource(mappedName = "/ConnectionFactory")
    ConnectionFactory factory;


    @Test
    public void testA() {
        System.out.println(factory);
    }
}
