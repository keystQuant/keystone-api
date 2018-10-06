FROM python:3.7

RUN apt-get install -f && \
    apt-get update && \
    apt-get install -y && \
    apt-get install -y python3-pip python3-dev && \
    pip3 install uwsgi

COPY . /app

WORKDIR /app

RUN pip3 install --upgrade pip && \
    pip3 install -q Django==2.0 && \
    pip3 install -r requirements.txt

RUN python manage.py makemigrations && \
    python manage.py migrate && \
    python manage.py collectstatic --noinput && \
    python tools.py clean-tasks

EXPOSE 8000

CMD ["uwsgi", "--ini", "/app/config/uwsgi.ini"]
