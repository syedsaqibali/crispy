FROM python:3
COPY ./requirements.txt /.
RUN pip install -r requirements.txt
RUN rm -f ./requirements.txt

RUN mkdir /crispy_project
WORKDIR /crispy_project
