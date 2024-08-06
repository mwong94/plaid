FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN prefect config set PREFECT_API_URL=$PREFECT_API_URL

# COPY ./src ./

# CMD [ "python", "app.py" ]

CMD [ "prefect", "server", "start" ]
