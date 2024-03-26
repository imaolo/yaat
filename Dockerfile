FROM python:3
WORKDIR /usr/src/app
EXPOSE 80
ENV PYTHONUNBUFFERED=1
RUN git clone https://github.com/imaolo/yaat.git
WORKDIR /usr/src/app/yaat
RUN pip install -e .