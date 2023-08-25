# Use an official Python runtime as the base image
FROM python:3.8

# Install AWS CLI
RUN pip install awscli

# Create a directory within the image
RUN mkdir /wave

# Set the working directory to the created directory
WORKDIR /wave

#create volume
VOLUME /wave

# Copy your application files into the container
COPY . .

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ARG cityname
ARG yearvalue

ENV city_name=${cityname}
ENV year_value=${yearvalue}

# Define the command to run your application (replace with your command)

CMD python main.py --city ${city_name} --year ${year_value}