FROM python:3.11

ARG YOUR_ENV

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=1.4.2

# System deps:
RUN pip install "poetry==$POETRY_VERSION"

# Copy only dependencies to cache them in docker layer
WORKDIR /code
COPY poetry.lock pyproject.toml /code/

# Project initialization:
RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi --no-root

# Creating folders, and files for a project:
COPY ./dp_plain_python /code/dp_plain_python
COPY ./config_aws.ini /code/config.ini

# Expose port
EXPOSE 8000

# Start webserver
CMD ["poetry", "run" ,"run-all"]
