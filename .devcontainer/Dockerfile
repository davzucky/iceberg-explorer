FROM mcr.microsoft.com/devcontainers/python:3.14

RUN pip install --no-cache-dir uv && uv --version

ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /workspace

CMD ["/bin/bash"]
