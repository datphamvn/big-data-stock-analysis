FROM python:3.8-slim-buster

WORKDIR /app

COPY . .

RUN python3 -m pip install -r requirements.txt

RUN chmod +r batch_processing.py

CMD [ "python3", "batch_processing.py"]
