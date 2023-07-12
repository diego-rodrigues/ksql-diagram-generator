    SET 'auto.offset.reset' = 'earliest';

    CREATE OR REPLACE STREAM stream_db_A1 (
        record_ID VARCHAR KEY,
        name VARCHAR,
        recordB_ID VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'topic-dbtable-A',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'AVRO'
    );

    CREATE OR REPLACE TABLE table_db_B1 (
        record_ID VARCHAR PRIMARY KEY,
        address VARCHAR,
        review VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'topic-dbtable-B',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'AVRO'
    );

    CREATE OR REPLACE TABLE table_db_A1_rk
    WITH (
        KAFKA_TOPIC = 'topic-dbtable-A_rk',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'JSON'
    ) AS SELECT
        recordB_ID,
        LATEST_BY_OFFSET(name)      AS name,
        LATEST_BY_OFFSET(recordID)  AS recordID
    FROM stream_db_A1 a
    GROUP BY recordB_ID
    EMIT CHANGES;


    CREATE OR REPLACE TABLE tableA1xtableB1_table
    WITH (
        KAFKA_TOPIC = 'topic-dbtableAxdbtableB',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'JSON'
    ) AS SELECT
        a.recordBID                 AS recordID,
        a.name                      AS name,
        b.address                   AS address,
        b.review                    AS review
    FROM table_db_A1_rk a
    INNER JOIN table_db_B1 b
        ON a.recordBID = b.record_ID
    EMIT CHANGES;