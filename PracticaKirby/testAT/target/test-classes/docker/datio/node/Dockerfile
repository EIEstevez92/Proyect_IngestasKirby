FROM node:4.8.4

 RUN mkdir /opt/restServer
 ADD package.json /opt/restServer
 ADD server.js /opt/restServer

 CMD cd /opt/restServer && npm install && npm start
