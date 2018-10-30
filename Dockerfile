FROM python:3.6
RUN apt-get update
RUN pip install --upgrade pip

COPY requirements.pip /tmp/
RUN pip install -r /tmp/requirements.pip

RUN apt-get clean

ENV PYTHONPATH /opt/application

COPY ./src /opt/application

CMD python3 $PYTHONPATH/main.py