FROM python:2
WORKDIR /usr/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV configs=test
ENTRYPOINT [ "python", "./event_processor.py" ]
CMD []
