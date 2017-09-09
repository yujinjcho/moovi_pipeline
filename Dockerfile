FROM python:2.7-alpine

WORKDIR /app

ADD . /app

RUN apk update && \
    apk add --virtual build-deps gcc python-dev musl-dev libffi-dev && \
    apk add postgresql-dev libxml2-dev libxslt-dev vim

RUN pip --default-timeout=1000 install -r requirements.txt

CMD ["sh", "run_luigi"]
