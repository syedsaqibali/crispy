FROM python:3
RUN mkdir /crispy_project
COPY . /crispy_project/.
WORKDIR /crispy_project
RUN pip install -r requirements.txt
