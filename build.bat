docker build -t mescorp-bot:v4 .
docker tag mescorp-bot:v4 mescorp-bot.cr.cloud.ru/mescorp-bot:v4
docker push mescorp-bot.cr.cloud.ru/mescorp-bot:v4