# Замеры хранилищ

Источник снимка: `https://api-web.nhle.com/v1/standings/2026-10-23`.
Клубов в снимке: 32. Дивизион для запроса: Metropolitan (8 клубов).
Повторов: 5. В таблицу попала медиана.

Вопрос один и тот же: записать снимок и выбрать клубы дивизиона, отсортировав по очкам.
MinIO отвечает фильтром в Python после чтения объекта. Отдельного языка запросов у бакета нет.
Метка Neo4j в замере — `BenchTeam`. Боевые узлы `Team` этот скрипт не удаляет.

| Хранилище | Запись, мс | Запрос, мс |
|---|---:|---:|
| PostgreSQL | 2.28 | 0.77 |
| MongoDB | 1.32 | 0.86 |
| MinIO | 8.2 | 3.38 |
| Neo4j | 14.08 | 6.54 |

## Журнал загрузок DAG

| DAG | Хранилище | Сущность | Записей | Байт | Мс |
|---|---|---|---:|---:|---:|
| nhl_standings_to_postgres | minio | nhl_standings | 32 | 63326 | 1101 |
| nhl_standings_to_postgres | postgres | nhl_standings | 32 | 0 | 98 |
| espn_nhl_to_minio | minio | espn_nhl | 8 | 88518 | 1787 |
| espn_nhl_to_minio | minio | espn_manifest | 8 | 1158 | 143 |
| nhl_games_to_mongodb | minio | nhl_game | 12 | 1962107 | 21109 |
| nhl_games_to_mongodb | mongodb | nhl_game | 12 | 0 | 358 |
| nhl_graph_to_neo4j | minio | nhl_graph_raw | 45 | 948901 | 37646 |
| nhl_graph_to_neo4j | neo4j | nhl_graph | 1350 | 0 | 2740 |
