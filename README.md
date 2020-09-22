# producer_s7
This java application is in charge of extracting some data from 38 Siemens PLCs (S7-300 and S7-1500), parse it to JSON format and send it to Kafka.
Data extraction is done using Apache PLC4X. The messages are sent to Apache Kafka through a Kafka Producer configured as idempotent and with some configurations to achieve high throughput.
