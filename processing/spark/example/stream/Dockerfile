FROM vsvale/spark_base_image:3.1.1

# using root user
USER root:root

# copy spark program
COPY ./*.py /app/
COPY ./*.yaml /app/

# set work directory
WORKDIR /app

# user
USER 1001