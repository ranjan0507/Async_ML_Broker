FROM gcc:latest

WORKDIR /app

COPY . .

RUN g++ -o my_broker src/server.cpp

EXPOSE 8080

CMD ["./my_broker"]