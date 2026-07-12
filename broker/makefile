CXX = g++
CXXFLAGS = -O3 -Wall -pthread

SRC = src/server.cpp
TARGET = src/my_broker

all: $(TARGET)

$(TARGET):	$(SRC)
		$(CXX) $(CXXFLAGS)	$(SRC)	-o	$(TARGET)

clean:	
		rm -f $(TARGET)

