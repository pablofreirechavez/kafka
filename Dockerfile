FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
# Aquí instalamos lo mínimo necesario para que tu .java funcione
RUN apt-get update && apt-get install -y maven
COPY . .
# Por ahora, solo queremos que esté listo para compilar
CMD ["tail", "-f", "/dev/null"]
