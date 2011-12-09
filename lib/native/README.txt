This is a placeholder to put Hadoop native libraries -it's a component
that contains platform-specific native code that significantly speeds up
data (de)compression. Since there are no maven artifacts for this component
the build process can't automatically download it.

These libraries are purely optional, and if they are missing Hadoop will
use corresponding pure Java components. The impact of native compression
becomes noticeable with larger datasets and weaker CPU-s - if you notice
that the CPU is routinely saturated when a job is sorting or reducing,
then using these libs may help.

Installation instructions
=========================
You can obtain the necessary files from a distribution package of Hadoop,
e.g. hadoop-0.20.2.tar.gz. Unpack this archive, and copy the content of
lib/native here, so that the layout looks like this:

<Nutch home>/lib/native/Linux-amd64-64/...
<Nutch home>/lib/native/Linux-i386-32/...

Local runtime
-------------
The build process will include these native libraries when preparing
the /runtime/local environment for running in local mode.

/runtime/local/bin/nutch knows how to use these libs - if they are
found and correctly used that's fine, however if they are not and you
see WARN, don't worry, however you will see lines like this in your logs:
 
15:36:02,126 WARN org.apache.hadoop.util.NativeCodeLoader: Unable to load
native-hadoop library for your platform... using builtin-java classes where
applicable
...
probably quite a few more of the same
...

Distributed runtime
-------------------
If you want to use this component in an existing Hadoop cluster (when using
/runtime/deploy artifacts) you need to make sure these files are placed in
Hadoop/lib/native directory on each node, and then restart the cluster. If
you installed the cluster from a distribution package of Hadoop then these
libraries should already be in the right place and you shouldn't need to do
anything else.
