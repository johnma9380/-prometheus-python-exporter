FROM python:2.7

WORKDIR /app
ADD . /app

RUN apt-get update && \
    apt-get install -y && \
    python -m pip install --upgrade pip && \
    pip install -r ./requirements.txt && \
    # aws configure set  aws_access_key_id  xxx && \ # aws設定
    # aws configure set  aws_secret_access_key xxx && \ # aws設定
    aws configure set region ap-northeast-1 && \
    apt-get clean

EXPOSE 9120

CMD ["python", "exporter_hook.py"]