FROM public.ecr.aws/lambda/python:3.8

COPY app.py requirements.txt ./

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["app.lambda_handler"]
