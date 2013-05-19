/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse.tika;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.tika.DOMBuilder;
import org.apache.nutch.parse.tika.DOMContentUtils;
import org.apache.nutch.parse.tika.TikaParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.xml.serializer.dom3.LSSerializerImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Unit tests for DOMContentUtils.
 */
public class DOMContentUtilsTest {

	private static final String[] testPages = {
			// 0.
			new String(
					"<html><head><title> title </title><script> script </script>"
							+ "</head><body> body <a href=\"http://www.nutch.org\">"
							+ " anchor </a><!--comment-->" + "</body></html>"),
			// 1.
			new String(
					"<html><head><title> title </title><script> script </script>"
							+ "</head><body> body <a href=\"/\">"
							+ " home </a><!--comment-->"
							+ "<style> style </style>"
							+ " <a href=\"bot.html\">" + " bots </a>"
							+ "</body></html>"),
			// 2.
			new String("<html><head><title> </title>" + "</head><body> "
					+ "<a href=\"/\"> separate this "
					+ "<a href=\"ok\"> from this" + "</a></a>"
					+ "</body></html>"),
			// 3.
			// this one relies on certain neko fixup behavior, possibly
			// distributing the anchors into the LI's-but not the other
			// anchors (outside of them, instead)! So you get a tree that
			// looks like:
			// ... <li> <a href=/> home </a> </li>
			// <li> <a href=/> <a href="1"> 1 </a> </a> </li>
			// <li> <a href=/> <a href="1"> <a href="2"> 2 </a> </a> </a> </li>
			new String("<html><head><title> my title </title>"
					+ "</head><body> body " + "<ul>"
					+ "<li> <a href=\"/\"> home" + "<li> <a href=\"1\"> 1"
					+ "<li> <a href=\"2\"> 2" + "</ul>" + "</body></html>"),
			// 4.
			// test frameset link extraction. The invalid frame in the middle
			// will be
			// fixed to a third standalone frame.
			new String("<html><head><title> my title </title>"
					+ "</head><frameset rows=\"20,*\"> "
					+ "<frame src=\"top.html\">" + "</frame>"
					+ "<frameset cols=\"20,*\">" + "<frame src=\"left.html\">"
					+ "</frame>" + "<frame src=\"invalid.html\"/>" + "</frame>"
					+ "<frame src=\"right.html\">" + "</frame>" + "</frameset>"
					+ "</frameset>" + "</body></html>"),
			// 5.
			// test <area> and <iframe> link extraction + url normalization
			new String(
					"<html><head><title> my title </title>"
							+ "</head><body>"
							+ "<img src=\"logo.gif\" usemap=\"#green\" border=\"0\">"
							+ "<map name=\"green\">"
							+ "<area shape=\"polygon\" coords=\"19,44,45,11,87\" href=\"../index.html\">"
							+ "<area shape=\"rect\" coords=\"128,132,241,179\" href=\"#bottom\">"
							+ "<area shape=\"circle\" coords=\"68,211,35\" href=\"../bot.html\">"
							+ "</map>"
							+ "<a name=\"bottom\"/><h1> the bottom </h1> "
							+ "<iframe src=\"../docs/index.html\"/>"
							+ "</body></html>"),
			// 6.
			// test whitespace processing for plain text extraction
			new String(
					"<html><head>\n <title> my\t\n  title\r\n </title>\n"
							+ " </head>\n"
							+ " <body>\n"
							+ "    <h1> Whitespace\ttest  </h1> \n"
							+ "\t<a href=\"../index.html\">\n  \twhitespace  test\r\n\t</a>  \t\n"
							+ "    <p> This is<span> a whitespace<span></span> test</span>. Newlines\n"
							+ "should appear as space too.</p><p>Tabs\tare spaces too.\n</p>"
							+ "    This\t<b>is a</b> break -&gt;<br>and the line after<i> break</i>.<br>\n"
							+ "<table>"
							+ "    <tr><td>one</td><td>two</td><td>three</td></tr>\n"
							+ "    <tr><td>space here </td><td> space there</td><td>no space</td></tr>"
							+ "\t<tr><td>one\r\ntwo</td><td>two\tthree</td><td>three\r\tfour</td></tr>\n"
							+ "</table>put some text here<Br>and there."
							+ "<h2>End\tthis\rmadness\n!</h2>\r\n"
							+ "         .        .        .         ."
							+ "</body>  </html>"),
			// 7.
			// test that <a rel=nofollow> links are not returned
			new String(
					"<html><head></head><body>"
							+ "<a href=\"http://www.nutch.org\" rel=\"nofollow\"> ignore </a>"
							+ "<a rel=\"nofollow\" href=\"http://www.nutch.org\"> ignore </a>"
							+ "</body></html>"),
			// 8.
			// test that POST form actions are skipped
			new String(
					"<html><head></head><body>"
							+ "<form method='POST' action='/search.jsp'><input type=text>"
							+ "<input type=submit><p>test1</p></form>"
							+ "<form method='GET' action='/dummy.jsp'><input type=text>"
							+ "<input type=submit><p>test2</p></form></body></html>"),
			// 9.
			// test that all form actions are skipped
			new String(
					"<html><head></head><body>"
							+ "<form method='POST' action='/search.jsp'><input type=text>"
							+ "<input type=submit><p>test1</p></form>"
							+ "<form method='GET' action='/dummy.jsp'><input type=text>"
							+ "<input type=submit><p>test2</p></form></body></html>"),
			// 10.
			new String("<html><head><title> title </title>" + "</head><body>"
					+ "<a href=\";x\">anchor1</a>"
					+ "<a href=\"g;x\">anchor2</a>"
					+ "<a href=\"g;x?y#s\">anchor3</a>" + "</body></html>"),
			// 11.
			new String("<html><head><title> title </title>" + "</head><body>"
					+ "<a href=\"g\">anchor1</a>"
					+ "<a href=\"g?y#s\">anchor2</a>"
					+ "<a href=\"?y=1\">anchor3</a>"
					+ "<a href=\"?y=1#s\">anchor4</a>"
					+ "<a href=\"?y=1;somethingelse\">anchor5</a>"
					+ "</body></html>"), };

	private static int SKIP = 9;

	private static String[] testBaseHrefs = { "http://www.nutch.org",
			"http://www.nutch.org/docs/foo.html", "http://www.nutch.org/docs/",
			"http://www.nutch.org/docs/", "http://www.nutch.org/frames/",
			"http://www.nutch.org/maps/", "http://www.nutch.org/whitespace/",
			"http://www.nutch.org//", "http://www.nutch.org/",
			"http://www.nutch.org/", "http://www.nutch.org/",
			"http://www.nutch.org/;something" };

	private static final DocumentFragment testDOMs[] = new DocumentFragment[testPages.length];

	private static URL[] testBaseHrefURLs = new URL[testPages.length];

	private static final String[] answerText = {
			"body anchor",
			"body home bots",
			"separate this from this",
			"body home 1 2",
			"",
			"the bottom",
			"Whitespace test whitespace test "
					+ "This is a whitespace test . Newlines should appear as space too. "
					+ "Tabs are spaces too. This is a break -> and the line after break . "
					+ "one two three space here space there no space "
					+ "one two two three three four put some text here and there. "
					+ "End this madness ! . . . .", "ignore ignore",
			"test1 test2", "test1 test2", "anchor1 anchor2 anchor3",
			"anchor1 anchor2 anchor3 anchor4 anchor5" };

	private static final String[] answerTitle = { "title", "title", "",
			"my title", "my title", "my title", "my title", "", "", "",
			"title", "title" };

	// note: should be in page-order
	private static Outlink[][] answerOutlinks;

	private static Configuration conf;
	private static DOMContentUtils utils = null;
	
	public static final Logger Logger = LoggerFactory.getLogger(DOMContentUtilsTest.class);

	public DOMContentUtilsTest(String name) {
	}

	private static void setup() throws Exception {
		conf = NutchConfiguration.create();
		conf.setBoolean("parser.html.form.use_action", true);
		utils = new DOMContentUtils(conf);
		TikaParser tikaParser = new TikaParser();
		tikaParser.setConf(conf);
		Parser parser = tikaParser.getTikaConfig().getParser("text/html");
		for (int i = 0; i < testPages.length; i++) {
			Metadata tikamd = new Metadata();

			HTMLDocumentImpl doc = new HTMLDocumentImpl();
			doc.setErrorChecking(false);
			DocumentFragment root = doc.createDocumentFragment();
			DOMBuilder domhandler = new DOMBuilder(doc, root);
			ParseContext context = new ParseContext();
			// to add once available in Tika
			//context.set(HtmlMapper.class, IdentityHtmlMapper.INSTANCE);
			try {
				parser.parse(new ByteArrayInputStream(testPages[i].getBytes()),
						domhandler, tikamd, context);
				testBaseHrefURLs[i] = new URL(testBaseHrefs[i]);
			} catch (Exception e) {
				e.printStackTrace();
				fail("caught exception: " + e);
			}
			testDOMs[i] = root;
			LSSerializerImpl lsi = new LSSerializerImpl();
			System.out.println("input " + i + ": '" + testPages[i] + "'");
			System.out.println("output " + i + ": '" + lsi.writeToString(root)
					+ "'");

		}
		answerOutlinks = new Outlink[][] {
				// 0
				{ new Outlink("http://www.nutch.org", "anchor"), },
				// 1
				{
				  new Outlink("http://www.nutch.org/", "home"),
				  new Outlink("http://www.nutch.org/docs/bot.html",
								"bots"), },
				// 2
				{
					new Outlink("http://www.nutch.org/", "separate this"),
					new Outlink("http://www.nutch.org/docs/ok", "from this"), },
				
				// 3	
				{   new Outlink("http://www.nutch.org/", "home"),
					new Outlink("http://www.nutch.org/docs/1", "1"),
					new Outlink("http://www.nutch.org/docs/2", "2"), },
				// 4	
				{
					new Outlink("http://www.nutch.org/frames/top.html", ""),
					new Outlink("http://www.nutch.org/frames/left.html", ""),
					new Outlink("http://www.nutch.org/frames/invalid.html",""),
					new Outlink("http://www.nutch.org/frames/right.html",""), 
				},
				// 5
				{ 
					new Outlink("http://www.nutch.org/maps/logo.gif", ""),
					new Outlink("http://www.nutch.org/index.html", ""),
					new Outlink("http://www.nutch.org/maps/#bottom", ""),
					new Outlink("http://www.nutch.org/bot.html", ""),
					new Outlink("http://www.nutch.org/docs/index.html", "") 
				},
				// 6
				{ new Outlink("http://www.nutch.org/index.html",
						"whitespace test"), 
				},
				// 7
				{},
				// 8
				{ new Outlink("http://www.nutch.org/dummy.jsp", "test2"), },
				// 9
				{},
				// 10 
				{ 
				 new Outlink("http://www.nutch.org/;x", "anchor1"),
				 new Outlink("http://www.nutch.org/g;x", "anchor2"),
				 new Outlink("http://www.nutch.org/g;x?y#s", "anchor3") 
				},
				// 11
				{
				 new Outlink("http://www.nutch.org/g;something","anchor1"),
				 new Outlink("http://www.nutch.org/g;something?y#s", "anchor2"),
				 new Outlink("http://www.nutch.org/;something?y=1", "anchor3"),
				 new Outlink("http://www.nutch.org/;something?y=1#s", "anchor4"),
				 new Outlink("http://www.nutch.org/?y=1;somethingelse", "anchor5") }
				};

	}

	private static boolean equalsIgnoreWhitespace(String s1, String s2) {
		StringTokenizer st1 = new StringTokenizer(s1);
		StringTokenizer st2 = new StringTokenizer(s2);

		while (st1.hasMoreTokens()) {
			if (!st2.hasMoreTokens()) {
			 Logger.info("st1+ '" + st1.nextToken() + "'");
				return false;
			}
			String st1Token = st1.nextToken();
			String st2Token = st2.nextToken();
			if (!st1Token.equals(st2Token)) {
			 Logger.info("st1:'" + st1Token + "' != st2:'" + st2Token + "'");
				return false;
			}
		}
		if (st2.hasMoreTokens()) {
			System.err.println("st2+ '" + st2.nextToken() + "'");
			return false;
		}
		return true;
	}

	@Test
	public void testGetText() throws Exception {
		if (testDOMs[0] == null)
			setup();
		for (int i = 0; i < testPages.length; i++) {
			StringBuffer sb = new StringBuffer();
			utils.getText(sb, testDOMs[i]);
			String text = sb.toString();
			assertTrue(
					"example " + i + " : expecting text: " + answerText[i]
							+ System.getProperty("line.separator")
							+ System.getProperty("line.separator")
							+ "got text: " + text,
					equalsIgnoreWhitespace(answerText[i], text));
		}
	}

	// won't work with Tika - the title is stored in the metadata but
	// not put in the XHTML representation
	@Test
	public void testGetTitle() throws Exception {
		if (testDOMs[0] == null)
			setup();
		for (int i = 0; i < testPages.length; i++) {
			StringBuffer sb = new StringBuffer();
			utils.getTitle(sb, testDOMs[i]);
			String title = sb.toString();
			assertTrue(
					"example " + i + " : expecting title: " + answerTitle[i]
							+ System.getProperty("line.separator")
							+ System.getProperty("line.separator")
							+ "got title: " + title,
					equalsIgnoreWhitespace(answerTitle[i], title));
		}
	}

	@Test
	public void testGetOutlinks() throws Exception {
		if (testDOMs[0] == null)
			setup();
		for (int i = 0; i < testPages.length; i++) {
			ArrayList<Outlink> outlinks = new ArrayList<Outlink>();
			if (i == SKIP) {
				conf.setBoolean("parser.html.form.use_action", false);
				utils.setConf(conf);
			} else {
				conf.setBoolean("parser.html.form.use_action", true);
				utils.setConf(conf);
			}
			utils.getOutlinks(testBaseHrefURLs[i], outlinks, testDOMs[i]);
			Outlink[] outlinkArr = new Outlink[outlinks.size()];
			outlinkArr = outlinks.toArray(outlinkArr);
			compareOutlinks(i, answerOutlinks[i], outlinkArr);
		}
	}

	private static final void appendOutlinks(StringBuffer sb, Outlink[] o) {
		for (int i = 0; i < o.length; i++) {
			sb.append(o[i].toString());
			sb.append(System.getProperty("line.separator"));
		}
	}

	private static final String outlinksString(Outlink[] o) {
		StringBuffer sb = new StringBuffer();
		appendOutlinks(sb, o);
		return sb.toString();
	}

	private static final void compareOutlinks(int test, Outlink[] o1,
			Outlink[] o2) {
		if (o1.length != o2.length) {
			assertTrue(
					"test " + test
							+ ", got wrong number of outlinks (expecting "
							+ o1.length + ", got " + o2.length + ")"
							+ System.getProperty("line.separator") + "answer: "
							+ System.getProperty("line.separator")
							+ outlinksString(o1)
							+ System.getProperty("line.separator") + "got: "
							+ System.getProperty("line.separator")
							+ outlinksString(o2)
							+ System.getProperty("line.separator"), false);
		}

		for (int i = 0; i < o1.length; i++) {
			if (!o1[i].equals(o2[i])) {
				assertTrue(
						"test " + test + ", got wrong outlinks at position "
								+ i + System.getProperty("line.separator")
								+ "answer: "
								+ System.getProperty("line.separator")
								+ o1[i].toString()
								+ System.getProperty("line.separator")
								+ "got: "
								+ System.getProperty("line.separator")
								+ o2[i].toString(), false);

			}
		}
	}
}
