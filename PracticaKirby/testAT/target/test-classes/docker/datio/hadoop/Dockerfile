FROM sequenceiq/hadoop-docker:2.7.1

ADD hdfsTestFiles/ /misc
ADD bootstrap.sh /etc/bootstrap.sh

RUN chmod +x "/etc/bootstrap.sh"

CMD ["/etc/bootstrap.sh", "-d"]