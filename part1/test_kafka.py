     1	from kafka import KafkaProducer
     2	
     3	producer = KafkaProducer(
     4	    bootstrap_servers="77.81.230.104:9092",
     5	    security_protocol="SASL_PLAINTEXT",
     6	    sasl_mechanism="PLAIN",
     7	    sasl_plain_username="admin",
     8	    sasl_plain_password="VawEzo1ikLtrA8Ug8THa",
     9	    request_timeout_ms=10000,
    10	)
    11	
    12	producer.send("athlete_event_results", b"test")
    13	producer.flush()
    14	print("OK")
    15	producer.close()
