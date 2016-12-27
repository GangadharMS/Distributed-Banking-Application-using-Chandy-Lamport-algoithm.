LIB_PATH=/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar
all: clean
	mkdir bin
	mkdir bin/client_classes
	mkdir bin/server_classes
	javac -classpath $(LIB_PATH) -d bin/client_classes/ src/FileHandlingClient.java src/FileStore.java src/RFile.java src/RFileMetadata.java src/Status.java src/StatusReport.java src/SystemException.java
	javac -classpath $(LIB_PATH) -d bin/server_classes/ src/FileHandlingServer.java src/FileHandler.java src/FileStore.java src/RFile.java src/RFileMetadata.java src/Status.java src/StatusReport.java src/SystemException.java


clean:
	rm -rf bin/

