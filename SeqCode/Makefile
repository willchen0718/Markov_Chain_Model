JFLAGS = -g
JC = javac
.SUFFIXES: .java .class
%.java %.class:
	$(JC) $(JFLAGS) $*.java
	
CLASSES = \
	  TrainingStage2.java \
	  MarkovChain.java \
	  MC_State.java 

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
