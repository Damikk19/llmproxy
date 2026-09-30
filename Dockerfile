FROM alpine:3.21.8

RUN apk add \
	py3-aiosqlite \
	py3-gpep517 \
	py3-installer \
	py3-mongo \
	py3-pip \
	py3-prometheus-client \
	py3-setuptools \
	python3 \
	sqlite

# Alpine's aiohttp lags behind security fixes (keep these pins in sync with
# .gitlab-ci.yml), and motor is not packaged at all.
RUN pip install --break-system-packages --no-cache-dir \
	aiohttp==3.14.3 \
	motor==3.5.0 \
	yarl==1.25.1

WORKDIR /root/llmproxy
COPY scripts scripts
COPY MANIFEST.in .
COPY pyproject.toml .
COPY llmproxy llmproxy

RUN gpep517 build-wheel --wheel-dir .dist --output-fd 3 3>&1 >&2
RUN python3 -m installer .dist/llmproxy-*.whl

RUN rm -rf /root/llmproxy

WORKDIR /

ENTRYPOINT ["python3", "-m", "llmproxy"]
