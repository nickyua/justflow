# syntax=docker/dockerfile:1.7

ARG PYTHON_BUILD_NAME=python:3.14.7-slim-trixie
ARG PYTHON_BUILD_DIGEST=sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
ARG PYTHON_RUNTIME_VERSION=3.14
ARG JUSTFLOW_BASE_IMAGE=justflow:local
ARG APPLICATION_VERSION=0.1.1
ARG APPLICATION_VIRTUAL_ENV=/opt/justflow/venv

FROM ${PYTHON_BUILD_NAME}@${PYTHON_BUILD_DIGEST} AS application-builder
ARG APPLICATION_VERSION
ARG APPLICATION_VIRTUAL_ENV
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY examples/host_application/pyproject.toml ./
COPY examples/host_application/src src
RUN python -m pip wheel --no-deps --wheel-dir /application-wheel . \
    && python -m venv --without-pip "${APPLICATION_VIRTUAL_ENV}" \
    && python -m pip --python "${APPLICATION_VIRTUAL_ENV}" install \
        --no-deps --no-index --find-links /application-wheel \
        "justflow-host-application-example==${APPLICATION_VERSION}"

FROM ${JUSTFLOW_BASE_IMAGE} AS runtime
ARG PYTHON_RUNTIME_VERSION
ARG APPLICATION_VERSION
ARG APPLICATION_VIRTUAL_ENV
ARG SOURCE_REVISION=unknown
ARG BUILD_DATE=unknown
ARG SOURCE_URL=unknown
LABEL org.opencontainers.image.title="Justflow host-application reference" \
      org.opencontainers.image.description="Immutable host-owned reference application for Justflow" \
      org.opencontainers.image.version="${APPLICATION_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.licenses="Apache-2.0"
USER root
COPY --from=application-builder \
    ${APPLICATION_VIRTUAL_ENV}/lib/python${PYTHON_RUNTIME_VERSION}/site-packages/ \
    ${APPLICATION_VIRTUAL_ENV}/lib/python${PYTHON_RUNTIME_VERSION}/site-packages/
COPY --from=application-builder --chmod=0555 \
    ${APPLICATION_VIRTUAL_ENV}/bin/host-application-config \
    ${APPLICATION_VIRTUAL_ENV}/bin/host-application-config
COPY --chown=root:root examples/host_application/src/host_application/configs /opt/justflow/config
RUN find /opt/justflow/config -type d -exec chmod 0555 {} + \
    && find /opt/justflow/config -type f -exec chmod 0444 {} +
ENV JUSTFLOW_HEALTHCHECK_URL=http://127.0.0.1:8080/readyz \
    JUSTFLOW_HEALTHCHECK_TIMEOUT_SECONDS=2
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD python -c 'import os,urllib.request; urllib.request.urlopen(os.environ["JUSTFLOW_HEALTHCHECK_URL"], timeout=float(os.environ["JUSTFLOW_HEALTHCHECK_TIMEOUT_SECONDS"])).close()'
USER 65532:65532
ENTRYPOINT ["uvicorn"]
CMD ["host_application.worker:create_worker_application", "--factory", "--host", "0.0.0.0", "--port", "8080"]
