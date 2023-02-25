FROM public.ecr.aws/lambda/python:3.8

COPY app.py requirements.txt ./

RUN pip install --upgrade pip --root-user-action=ignore
RUN pip install -r requirements.txt --root-user-action=ignore

CMD ["app.lambda_handler"]
