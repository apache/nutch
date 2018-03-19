Welcome to Anthelion!

This project provides the implementation of a novel selection strategy for focused crawlers together with a crawling simulation environment to test and evaluate different configurations. It makes use of online learning methods in combination with bandit-based explore/exploit approaches to predict data-rich web pages based on the context of the page as well as using feedback from the extraction of metadata from previously seen pages.
We applied this strategy to gather content from web-pages embedding Microdata, RDFa or Microformats within their HTML and provide structured data. The results were presented in "Focused Crawling for Structured Data" at CIKM 2014 in Shanghai, China (https://labs.yahoo.com/_c/uploads/anthelion.pdf).

## 3rd Party Library Information

Anthelion uses several 3rd party open source libraries.
This file summarizes the tools used, their purpose, and the licenses under which they're released.
This file also gives a basic introduction to building and using Anthelion.

Except as specifically stated below, the 3rd party software packages are not distributed as part of
this project, but instead are separately downloaded from the respective provider. As the project 
uses Maven, the necessary libraries and packages will be downloaded automatically by Maven when build.

* MOA version 2012.08 (GNU General Public License - http://www.gnu.org/licenses/lgpl.html)
  MOA is the most popular open source framework for data stream mining. It includes a collection of machine learning algorithms (classification, regression, clustering, outlier detection, concept drift detection, and recommender systems) and tools for evaluation. 
  http://moa.cms.waikato.ac.nz/

* Weka-Package version 2012.08 (GNU General Public License - http://www.gnu.org/licenses/lgpl.html)
  Weka is a collection of machine learning algorithms for data mining tasks.  Weka contains tools for data pre-processing, classification, regression, clustering, association rules, and visualization. Package is extended by MOA.
  http://www.cs.waikato.ac.nz/ml/weka/
  
* Fastutil version 6.5.7 (Apache License 2 - http://www.apache.org/licenses/LICENSE-2.0)
  Extends the Java Collections Framework by providing type-specific maps, sets, lists and queues with a small memory footprint and fast access and insertion
  http://fastutil.di.unimi.it/

* SizeOfAg version 1.0.0 (GNU Lesser General Public License - http://www.gnu.org/licenses/lgpl.html)
  Java Agent that allows you to determine the size of Java objects from within the JVM at runtime. This makes it very useful for developing Java frameworks that take memory constraints into account. Used by MOA to determine the size of the current model and keep given boundaries.
  https://code.google.com/p/sizeofag/ (Included in the Anthelion distribution.)
  
* Commons-math version 2.2 (GNU Lesser General Public License - http://www.gnu.org/licenses/lgpl.html)
  Commons Math is a library of lightweight, self-contained mathematics and statistics components addressing the most common problems not available in the Java programming language or Commons Lang.
  http://commons.apache.org/proper/commons-math/

* Dwslib version 1.1-SNAPSHOT (Creative Commons Attribution-NonCommercial 4.0 International License - https://creativecommons.org/licenses/by-nc/4.0/)
  Toolbox by the Data and Web Science Group of the University of Mannheim, Germany. Providing utility function for various tasks, like parallel processing frameworks and input and output handlers. 
  https://github.com/dwslab/dwslib
  
## Getting Started

After you have build the project, navigate to the \target folder. To simulate a crawl, you simply have to execute the main function of the class CCFakeCrawler. 
You will be presented with a set of actions you can execute to initialize, run and observe the simulation.

Start the simulation:

  java -Xmx15G -cp ant.jar org.apache.nutch.anthelion.simulation.CCFakeCrawler [indexfile] [networkfile] [labelfile] [propertiesfile] [resultlogfile]

### Necessary files:

* index: the mapping between ID and URL
* network: the graph including the IDs from the index
* label: list of the IDs which fulfil the target function
* properties: configuration file (a set of configuration files can be found in the resource folder of the distribution)
* result: the location where the information about the performance and the crawling process are stored

The files which we used to measure the performance when crawling for HTML pages including Microdata, Microformats and RDFa can be found on the dedicated page of the WebDataCommons project: http://webdatacommons.org/structureddata/anthelion/

### Available actions within the simulation process:

* Run "init" to initialize the crawler (loading the network, labels and create the features).
* Run "start" to start the crawler and simulate a crawl. Output is written to the result.log
* Use "stop" to stop the simulation
* Run "exit" to shut down 
* Use "status" to observe the crawling process.
