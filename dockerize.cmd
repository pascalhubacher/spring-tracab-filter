docker build -t spring-tracab-filter:0.0.1 -t spring-tracab-filter:latest .

REM if it runs on localhost
REM docker run -it -p 8080:8080 spring-tracab-filter:latest
REM if it runs on 192.168.1.100
REM docker run --env DATAPLATFORM_IP=192.168.1.100 -it -p 8080:8080 spring-tracab-filter:latest