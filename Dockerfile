FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

EXPOSE 8000

WORKDIR /app/basic

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]