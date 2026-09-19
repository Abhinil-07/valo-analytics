FROM evidencedev/serve:latest
COPY --chown=evidence:evidence . /project
# Automatically ensure production connection.yaml is in place
RUN if [ -f /project/connection.yaml.prod ] && [ ! -f /project/connection.yaml ]; then cp /project/connection.yaml.prod /project/connection.yaml; fi
