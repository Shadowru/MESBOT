docker build -t mescorp-bot:v3 .
docker tag mescorp-bot:v3 mescorp-bot.cr.cloud.ru/mescorp-bot:v3
docker push mescorp-bot.cr.cloud.ru/mescorp-bot:v3