# Use Google’s Python39 Flex-Template launcher base
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

WORKDIR /templates

# 1) Install your Python dependencies at build time
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# 2) Copy your streaming pipeline code
COPY halt_detection_beam_job.py .

# 3) Tell the launcher which script to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/templates/halt_detection_beam_job.py"

# 4) Tell Dataflow to use your image as the SDK container
ENV FLEX_TEMPLATE_PYTHON_CONTAINER_IMAGE="gcr.io/fluted-cogency-403608/cab-idle:latest"