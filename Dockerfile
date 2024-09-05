FROM jinnlynn/uwsgi:py3.12

COPY app.py requirements.txt /app/opt/
COPY config.toml /app/etc/

RUN set -ex && \
    pip install -r /app/opt/requirements.txt

ENV FASTLY_CONFIG=/app/etc/config.toml

WORKDIR /app/opt/
