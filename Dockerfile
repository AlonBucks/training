FROM python:3.8

# Create app directory
WORKDIR /app

ENV FLASK_APP app.py
ENV FLASK_RUN_HOST 0.0.0.0

# Install app dependencies
COPY /requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY / /app
