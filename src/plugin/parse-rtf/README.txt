Prereqs: JDK 1.4+ and javacc version 3.2+

This document describes how to create rtf-parser.jar file as used by Nutch.

Source files are contained in:

http://www.cobase.cs.ucla.edu/pub/javacc/rtf_parser_src.jar

Create a new directory with the following files in:

	LICENCE
	RTFParser.jj
	RTFParserDelegate.java

cd into this new directory create a src directory
	
	$mkdir src
	
copy RTFParser.jj RTFParserDelegate.java into this src directory

	$cp RTFParser.jj RTFParserDelegate.java src/
	
now cd into this src directory and generate the javacc classes for the parser
and then cd out again

	$cd src
	$javacc RTFParser.jj
	$cd ..
	
now compile all the source and generated files

	$javac -d . src/*.java
	
(optional) remove the generated source

	$rm -rf src # (optional)
	
finally create the jar archive of all the salient files

	$jar -cvf rtf-parser.jar com/ LICENCE RTFParser*
	
--Andy Hedges

Credits:

Thanks to Eric Friedman for writing this javacc grammar file.

