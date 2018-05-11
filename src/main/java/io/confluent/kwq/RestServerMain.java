/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kwq;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.MalformedURLException;
import java.util.Properties;

public class RestServerMain {

  // TODO: sort out the path and packaging into the jar
  static String resourcesFolder = "/Users/neil/IdeaProjects/kwq/src/main/resources";
  private static Server server;


  public static void main(String[] args) throws Exception {
    initialize();
    start();
    join();
    destroy();
  }

  public static void initialize() throws MalformedURLException {
    server = new Server(8080);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.setBaseResource(Resource.newResource(resourcesFolder + "/ui"));
    context.setWelcomeFiles(new String[]{"index.html"});
    server.setHandler(context);

    // http://localhost:8080/kwq
    ServletHolder apiServlet = context.addServlet(ServletContainer.class, "/*");

    apiServlet.setInitParameter(ServerProperties.PROVIDER_CLASSNAMES, KwqRestEndpoint.class.getCanonicalName());
    apiServlet.setInitParameter(ServerProperties.APPLICATION_NAME, KwqRestEndpoint.class.getCanonicalName());
    apiServlet.setInitOrder(0);
    // configure swagger openapi path scanning
    apiServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES,
            "io.confluent.kwq,io.swagger.v3.jaxrs2.integration.resources");


    // TODO: make openapi.json handle paths properly (i.e. /api/kwq )
    // http://localhost:8080/openapi.json
    ServletHolder swaggerHolder = new ServletHolder("swaggerResources", DefaultServlet.class);
    swaggerHolder.setInitParameter("dirAllowed","true");
    swaggerHolder.setInitParameter("pathInfoOnly","true");
    swaggerHolder.setInitParameter("resourceBase", resourcesFolder + "/swagger");
    context.addServlet(swaggerHolder, "/swagger/*");

    // TODO: make openapi.json handle paths properly (i.e. /api/kwq )
    // http://localhost:8080/api/openapi.json
    ServletHolder uiHolder = new ServletHolder("uiResources", DefaultServlet.class);
    uiHolder.setInitParameter("dirAllowed","true");
    uiHolder.setInitParameter("pathInfoOnly","true");
    uiHolder.setInitParameter("resourceBase", resourcesFolder + "/ui");
    context.addServlet(uiHolder, "/ui/*");


    // Lastly, set the default servlet for root content (always needed, to satisfy servlet spec)
    // nb: It is important that this is last.
    ServletHolder holderDef = new ServletHolder("default", DefaultServlet.class);
    context.addServlet(holderDef,"/");


    // TODO: setup properties properly
    Properties properties = new Properties();

    properties.put("bootstrap.servers", System.getProperty("bootstrap.servers", "localhost:9092"));

    registerLifecycleHandler(apiServlet, properties);
  }

  private static void registerLifecycleHandler(ServletHolder apiServlet, Properties properties) {
    apiServlet.addLifeCycleListener(new LifeCycle.Listener() {
      @Override
      public void lifeCycleStarting(LifeCycle lifeCycle) {
        try {
          KwqInstance.getInstance(properties);
        } catch (Throwable badError) {
          System.err.println("Fatal error during startup");
          badError.printStackTrace();
//          server.destroy();
          System.exit(-1);
        }
      }

      @Override
      public void lifeCycleStarted(LifeCycle lifeCycle) {
      }

      @Override
      public void lifeCycleFailure(LifeCycle lifeCycle, Throwable throwable) {
      }

      @Override
      public void lifeCycleStopping(LifeCycle lifeCycle) {
      }

      @Override
      public void lifeCycleStopped(LifeCycle lifeCycle) {
      }
    });
  }


  public static void start() {
    try {
      server.start();
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
  }
  public static void destroy() {
    server.destroy();
  }

  public static void join() {
    try {
      server.join();
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
  }

  public static void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
