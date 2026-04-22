FROM ghcr.io/astral-sh/uv:0.5.2 AS uv 

FROM public.ecr.aws/lambda/python:3.13 

ENV UV_COMPILE_BYTECODE=1 
ENV UV_LINK_MODE=copy 

RUN --mount=type=cache,target=/root/.cache/uv \
	--mount=type=cache,target=/root/.cache/pip \
	--mount=type=bind,from=uv,source=/uv,target=/bin/uv \
	--mount=type=bind,source=pyproject.toml,target=pyproject.toml \
	--mount=type=bind,source=uv.lock,target=uv.lock \
	uv export --format=requirements-txt --no-dev --frozen -o requirements.txt && \
	uv pip install -r requirements.txt --system && \
	rm requirements.txt 

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.handler" ]
