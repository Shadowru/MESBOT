docker build -t mescorp-bot:v5 .
docker tag mescorp-bot:v5 mescorp-bot.cr.cloud.ru/mescorp-bot:v5
docker push mescorp-bot.cr.cloud.ru/mescorp-bot:v5