#!/bin/bash

uvicorn main:api --host 0.0.0.0 --port 5000 --reload

exec "$@"