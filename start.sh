#!/usr/bin/env bash
service nginx start
gunicorn \
        --user $(id -u www-data) \
        --group $(id -g www-data) \
        --umask "0022" \
        --workers 2 \
        --timeout 1200 \
        --bind unix:/tmp/uvicorn.sock \
        --error-logfile - \
        -k uvicorn.workers.UvicornWorker \
        cota_tvier_adapter:app