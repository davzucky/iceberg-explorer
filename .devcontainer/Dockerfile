FROM mcr.microsoft.com/devcontainers/python:3.14

RUN uv --version

RUN pip install --no-cache-dir uv

ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /workspace

CMD ["/bin/bash"]
