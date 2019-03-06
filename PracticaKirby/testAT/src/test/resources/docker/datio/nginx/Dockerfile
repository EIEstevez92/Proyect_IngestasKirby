FROM nginx:1.13.1-alpine
RUN rm /etc/nginx/conf.d/*
COPY nginx.conf /etc/nginx/conf.d/nginx.conf
ADD schema /www/data/schema