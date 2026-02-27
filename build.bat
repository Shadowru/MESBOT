docker build -t mescorp-bot:v1 .
docker tag mescorp-bot:v1 mescorp-bot.cr.cloud.ru/mescorp-bot:v1
docker push mescorp-bot.cr.cloud.ru/mescorp-bot:v1