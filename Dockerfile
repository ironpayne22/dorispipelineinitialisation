FROM python

WORKDIR /usr/src/app

COPY requirements.txt .

RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "pipelineinitialisation.py" ]