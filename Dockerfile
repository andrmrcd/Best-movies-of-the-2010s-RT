FROM python:3.10

# Set working directory within container
WORKDIR /app

# Copy requirements and .env file to container
COPY requirements.txt ./

# Set environmental variables
# ENV VAR_NAME=value

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire app to container
COPY . /app

# Set command to run script when container starts
CMD ["python", "main.py"]

