FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Настраиваем таймзону (ВАЖНО для напоминаний!)
ENV TZ="Europe/Moscow"
RUN apt-get update && apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY bot.py .

# Запускаем бота
CMD ["python", "bot.py"]