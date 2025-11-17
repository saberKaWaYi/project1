FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir

COPY . /app

EXPOSE 8000

WORKDIR /app/basic

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]