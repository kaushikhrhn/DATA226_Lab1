FROM apache/superset:6.1.0-py311

USER root
RUN uv pip install --python /app/.venv/bin/python snowflake-sqlalchemy

COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py
COPY --chown=superset:superset superset-init.sh /app/superset-init.sh
RUN chmod 755 /app/superset-init.sh

USER superset
