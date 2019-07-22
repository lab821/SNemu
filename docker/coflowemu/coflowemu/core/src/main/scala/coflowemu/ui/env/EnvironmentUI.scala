/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package coflowemu.ui.env

import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConversions._
import scala.util.Properties
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import coflowemu.ui.JettyUtils._
import coflowemu.ui.UIUtils
import coflowemu.ui.Page.Environment


private[coflowemu] class EnvironmentUI() {

  def getHandlers = Seq[(String, Handler)](
    ("/environment", (request: HttpServletRequest) => envDetails(request))
  )

  def envDetails(request: HttpServletRequest): Seq[Node] = {
    val jvmInformation = Seq(
      ("Java Version", "%s (%s)".format(Properties.javaVersion, Properties.javaVendor)),
      ("Java Home", Properties.javaHome),
      ("Scala Version", Properties.versionString),
      ("Scala Home", Properties.scalaHome)
    ).sorted
    def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    def jvmTable =
      UIUtils.listingTable(Seq("Name", "Value"), jvmRow, jvmInformation, fixedWidth = true)

    val properties = System.getProperties.iterator.toSeq
    val classPathProperty = properties.find { case (k, v) =>
      k.contains("java.class.path")
    }.getOrElse(("", ""))
    val varysProperties = properties.filter(_._1.startsWith("varys")).sorted
    val otherProperties = properties.diff(varysProperties :+ classPathProperty).sorted

    val propertyHeaders = Seq("Name", "Value")
    def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
    val varysPropertyTable =
      UIUtils.listingTable(propertyHeaders, propertyRow, varysProperties, fixedWidth = true)
    val otherPropertyTable =
      UIUtils.listingTable(propertyHeaders, propertyRow, otherProperties, fixedWidth = true)

    val content =
      <span>
        <h4>Runtime Information</h4> {jvmTable}
        <h4>Varys Properties</h4>
        {varysPropertyTable}
        <h4>System Properties</h4>
        {otherPropertyTable}
      </span>

    UIUtils.headerVarysPage(content, "Environment", Environment)
  }
}
