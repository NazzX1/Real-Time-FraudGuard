FROM python:3.9-slim



WORKDIR /app


RUN apt-get update && apt-get install -y netcat-openbsd


COPY . /app


RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt


COPY wait-for-it.sh  /usr/local/bin/wait-for-it.sh


ARG script=normal_transaction_producer.py



ENV SCRIPT_NAME=${script}


RUN chmod +x /usr/local/bin/wait-for-it.sh


CMD ["sh", "-c", "/usr/local/bin/wait-for-it.sh broker 9092 -- python /app/${SCRIPT_NAME}"]