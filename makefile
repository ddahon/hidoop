#
# A simple makefile for compiling three java classes
#

# define a makefile variable for the java compiler
#
JCC = javac

# define a makefile variable for compilation flags
# les .class sont stockés dans bin et sont ajoutées au CLASSPATH
#
JFLAGS = -d bin/ -classpath bin/

# typing 'make' will invoke the first target entry in the makefile 
# (the default one in this case)
#
default: hdfs ordo

formats: 
		$(JCC) $(JFLAGS) src/formats/*.java

config: 
		$(JCC) $(JFLAGS) src/config/*.java

hdfs: formats
		$(JCC) $(JFLAGS) src/hdfs/*.java

map: formats
		$(JCC) $(JFLAGS) src/map/*.java

ordo: formats config hdfs map
		$(JCC) $(JFLAGS) src/ordo/*.java

# To start over from scratch, type 'make clean'.  
# Removes all .class files, so that the next make rebuilds them
#
clean: 
		$(RM) -R bin/*/*.class