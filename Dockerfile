FROM openjdk:8-jre
MAINTAINER biteeniu <biteeniu@gmail.com>
WORKDIR /usr/src
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
COPY target/rabbitmq-delay-queue-v1.0.0-jar-with-dependencies.jar /usr/src/rabbitmq-delay-queue.jar
CMD ["java", "-jar", "/usr/src/rabbitmq-delay-queue.jar"]