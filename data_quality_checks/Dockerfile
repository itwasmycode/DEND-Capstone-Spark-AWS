FROM public.ecr.aws/lambda/python:3.8

RUN echo "deltarpm=0" >> /etc/yum.conf && \
    yum update -y && \
    yum install -y python3-virtualenv && \
    yum clean all && \
    rm -rf /var/cache/yum

COPY app.py requirements.txt ./

RUN python3 -m venv /app-env \
  && source /app-env/bin/activate \
  && pip3 install --no-cache-dir -r requirements.txt

CMD ["app.lambda_handler"]
