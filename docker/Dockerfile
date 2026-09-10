# syntax=docker/dockerfile:1.7

ARG PYTHON_BASE_NAME=python:3.14.7-slim-trixie
ARG PYTHON_BASE_DIGEST=sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
ARG PYTHON_RUNTIME_VERSION=3.14
ARG NODE_BUILD_IMAGE=node:24.11.1-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57

FROM ${NODE_BUILD_IMAGE} AS admin-assets
WORKDIR /build
COPY ui/admin/package.json ui/admin/package-lock.json ui/admin/
RUN npm --prefix ui/admin ci
COPY ui/admin/graph-frame.html ui/admin/index.html ui/admin/tsconfig.json ui/admin/vite.config.ts ui/admin/
COPY ui/admin/src ui/admin/src
COPY packages/justflow-admin packages/justflow-admin
RUN npm --prefix ui/admin run build

FROM ${PYTHON_BASE_NAME}@${PYTHON_BASE_DIGEST} AS wheel-builder
ARG JUSTFLOW_VERSION=0.1.1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY constraints/container.txt constraints/container.txt
COPY LICENSE NOTICE README.md pyproject.toml ./
COPY src src
COPY packages/justflow-admin packages/justflow-admin
COPY --from=admin-assets /build/packages/justflow-admin/src/justflow_admin/assets/dist \
    packages/justflow-admin/src/justflow_admin/assets/dist
RUN python -m pip install build==1.5.0 \
    && python -m build --wheel --outdir /dist \
    && python -m build packages/justflow-admin --wheel --outdir /dist \
    && python -m pip wheel \
        --constraint constraints/container.txt \
        --find-links /dist \
        --wheel-dir /wheelhouse \
        "justflow[all]==${JUSTFLOW_VERSION}"

FROM ${PYTHON_BASE_NAME}@${PYTHON_BASE_DIGEST} AS runtime
ARG PYTHON_BASE_NAME
ARG PYTHON_BASE_DIGEST
ARG PYTHON_RUNTIME_VERSION
ARG JUSTFLOW_VERSION=0.1.1
ARG SOURCE_REVISION=unknown
ARG BUILD_DATE=unknown
ARG SOURCE_URL=unknown
ARG RUNTIME_UID=65532
ARG RUNTIME_GID=65532
LABEL org.opencontainers.image.title="Justflow" \
      org.opencontainers.image.description="Immutable Justflow worker and API runtime" \
      org.opencontainers.image.version="${JUSTFLOW_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="${PYTHON_BASE_NAME}" \
      org.opencontainers.image.base.digest="${PYTHON_BASE_DIGEST}"
ENV PATH="/opt/justflow/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JUSTFLOW_PATHS__CONFIG_DIR=/opt/justflow/config \
    JUSTFLOW_PATHS__CATALOG_DIR=/var/lib/justflow
RUN groupadd --gid "${RUNTIME_GID}" justflow \
    && useradd --uid "${RUNTIME_UID}" --gid "${RUNTIME_GID}" \
        --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin justflow \
    && python -m venv /opt/justflow/venv \
    && mkdir -p /opt/justflow/config /var/lib/justflow \
    && chown -R root:root /opt/justflow \
    && chown -R "${RUNTIME_UID}:${RUNTIME_GID}" /var/lib/justflow
COPY --from=wheel-builder /wheelhouse /wheelhouse
RUN /opt/justflow/venv/bin/python -m pip install \
        --find-links /wheelhouse \
        --no-index \
        "justflow[all]==${JUSTFLOW_VERSION}" \
    && /opt/justflow/venv/bin/python -m pip check \
    && /opt/justflow/venv/bin/python -m pip uninstall --yes pip \
    && /usr/local/bin/python -m pip uninstall --yes pip \
    && rm -rf /wheelhouse "/usr/local/lib/python${PYTHON_RUNTIME_VERSION}/ensurepip"
WORKDIR /opt/justflow
VOLUME ["/var/lib/justflow"]
USER ${RUNTIME_UID}:${RUNTIME_GID}
ENTRYPOINT ["python", "-m", "justflow"]
CMD ["--help"]
