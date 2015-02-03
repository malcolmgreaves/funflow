# funflow
Provides a functional description of a workflow: a dependent series of big-data computations.


==========================================================================================
Using sbt for building, tests, running programs, packaging, managing dependencies etc.
==========================================================================================
We include a sbt script in the project. This script will download and install the appropriate version of sbt and then start it. The script behaves nearly identicly to an installed sbt. So use ./sbt or sbt as you desire!

We recommend using the following sbt options:
	 SBT_OPTS="-Xmx2G -XX:MaxPermSize=724M  -XX:+UseConcMarkSweepGC  -XX:+CMSClassUnloadingEnabled

These are the sbt commands used in this project:
*  test => runs unittests in src/test
*  scalariformFormat => runs automatic code formatting (all code in the master branch *must* be formatted) (test:scalariformFormat formats tests)
*  compile => compiles code in src/main (test:compile complies tests)
*  pack => packages all dependencies and the project code into a folder and creates a shell script that allows one to execute main() methods in the project. Used for installing this project.
*  update => downloads all dependencies
*  reload => When in an interactive sbt session, reload will parse and load the build.sbt file. This is very useful when updating dependencies or adding plugins. (start an interactive sessions by invoking sbt with no commands: ./sbt)
*  gen-idea => makes project files for Intellij IDEA 
*  eclipse => makes project files for Eclipse
