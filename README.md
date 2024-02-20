# bot_2l
Проект бота 2й линии автомакон


## Work with Liquibase
Liquibase запускается в контейнере, для первого запуска можно использовать liquibase_docker_start.ps1, - далее перезапускать контейнер до настройки CI/CD. Changelog лежит в db/changelog.
Аккумулятивный файл чендж логов - db.changelog-master.xml.

