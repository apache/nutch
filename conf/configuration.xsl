<?xml version="1.0"?>
<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="html"/>
<xsl:template match="configuration">
<html>
 <head>
  <title>Nutch Configuration Properties</title>
  <meta charset="utf-8"/>
  <style>
    table { width: 100%; table-layout: fixed; }
    th,td { padding: 0.2em 0.5em; }
    td { overflow:hidden; vertical-align:top; }
    th { background-color: #e0e0e0; }
    tr { background-color: #f0f0f0; }
    tr:nth-child(odd) { background-color: #fcfcfc; }
    th.name { width: 20% }
    th.value { width: 30% }
    th.description { width: 50% }
  </style>
 </head>
<body>
<table>
 <thead>
  <tr>
   <th class="name">Nutch Property Name</th>
   <th class="value">Default Value</th>
   <th class="description">Description</th>
  </tr>
 </thead>
 <tbody>
<xsl:for-each select="property">
  <tr>
   <td><a name="{name}"><xsl:value-of select="name"/></a></td>
   <td><xsl:value-of select="value"/></td>
   <td><xsl:value-of select="description"/></td>
  </tr>
</xsl:for-each>
 </tbody>
</table>
</body>
</html>
</xsl:template>
</xsl:stylesheet>
