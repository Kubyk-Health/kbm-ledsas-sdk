# The LEDSAS SDK and its runtime dependencies come pre-installed in this base
# image, and the base image tag IS the SDK version. Do not install the SDK again.
# To move the service to another SDK release, change the tag.
FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6

# An image inherits the labels of the image it is built from, and the base
# image's labels describe the base image (its name, version, license and
# links), not this service. They are cleared here: add your service's own
# (title, version, licenses, ...) to this instruction.
LABEL org.opencontainers.image.title="" \
      org.opencontainers.image.description="" \
      org.opencontainers.image.version="" \
      org.opencontainers.image.revision="" \
      org.opencontainers.image.created="" \
      org.opencontainers.image.licenses="" \
      org.opencontainers.image.url="" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.source="" \
      org.opencontainers.image.vendor=""

WORKDIR /app

# The base image runs as the non-root user kbmuser (uid 1000, gid 1000). Switch
# to root only to install this service's own dependencies, then switch back.
USER root
# /app (created by the base image for kbmuser) becomes root's, so the running
# service can neither change nor replace its code here. The service writes no
# files in /app; give it /tmp (or a mounted volume) for scratch files.
RUN chown root:root /app && chmod 0755 /app
COPY requirements.txt .
RUN PIP_ROOT_USER_ACTION=ignore pip install --no-cache-dir -r requirements.txt && pip check

# The code stays owned by root (read-only for kbmuser).
COPY main.py .

# Last root step: remove the package installer from the final image. It is only
# needed while building, never at runtime. Keep every pip command above this line
# (below it pip is gone and a pip command fails the build), and install into the
# base image's Python as above, not into a virtual environment: the check fails
# the build if an installed copy of pip remains in the image.
RUN PIP_ROOT_USER_ACTION=ignore python -m pip uninstall --yes pip \
 && ! python -c "import pip" 2>/dev/null \
 && ! find / -xdev \( -path '*/site-packages/pip' -o -name 'pip-*.dist-info' \) -print | grep .

# Back to kbmuser, by number: Kubernetes runAsNonRoot can verify a numeric user
# only (with a name it refuses the pod unless runAsUser is set).
USER 1000:1000
CMD ["python", "main.py"]
