CREATE DATABASE IF NOT EXISTS exchange;

--таблицы сырых данных из источника
CREATE TABLE IF NOT EXISTS exchange.input_rates
(
	currency String,
    timestamp Date,
    rate Float64,
    insertTime DateTime('Europe/Moscow') DEFAULT now(),
    partKey DEFAULT toYYYYMMDD(now())

) ENGINE = MergeTree
PARTITION BY partKey
ORDER BY (timestamp, currency)
TTL toStartOfDay(insertTime) + INTERVAL 1 DAY;

--вспомогательная таблица
CREATE TABLE IF NOT EXISTS exchange.tmp_rates
(
    currency String,
    timestamp Date,
    rate Float64,
    insertTime DateTime
)   ENGINE MergeTree
    PARTITION BY timestamp
    ORDER BY currency;
--таблица постоянного хранения
CREATE TABLE IF NOT EXISTS exchange.rates
(
    currency String,
    timestamp Date,
    rate Float64,
    insertTime DateTime
)   ENGINE MergeTree
    PARTITION BY timestamp
    ORDER BY currency;
