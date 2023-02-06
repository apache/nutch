<?xml version="1.0" encoding="UTF-8"?>

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

<!-- This file will transform a book.html to an xml document compounded of 
	specific fields. Each field will then be indexed (by default) -->
<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


	<xsl:template match="/">
		<documents>
			<document>

				<field name="title">
					<xsl:value-of select="/HTML/BODY/H1" />
				</field>

				<field name="description">
					<xsl:value-of select="//DIV[@id='description']" />
				</field>

				<field name="isbn">
					<xsl:variable name="fullDivText"
						select="//DIV[starts-with(text(), 'Isbn:')]/text()" />
					<xsl:value-of select="substring-after($fullDivText, 'Isbn: ')" />
				</field>

				<!-- Adding several Author fields -->
				<xsl:for-each select="/HTML/BODY/UL[starts-with(text(),'Authors')]/LI">
					<field name="author">
						<xsl:value-of select="." />
					</field>
				</xsl:for-each>

				<field name="price">
					<xsl:variable name="fullSpanText"
						select="//SPAN[starts-with(text(), 'Price:')]/text()" />
					<xsl:value-of select="substring-after($fullSpanText, 'Price: ')" />
				</field>

				<field name="collection">
					<xsl:value-of select="//DIV[@class='.collection']" />
				</field>


			</document>
		</documents>
	</xsl:template>

</xsl:stylesheet>