from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
file_path = '/Users/hasithamaganti/Documents/xnode-project/bank.csv'

start_time = time.time()
count = 0

try:
    with open(file_path, 'r') as file:
        for line in file:
            producer.send('dataset-topic', value=line.strip().encode('utf-8'))
            count += 1
            if count % 10000 == 0:
                print(f"{count} messages sent...")

    producer.flush()
    print(f"\n Finished sending {count} messages.")
except FileNotFoundError:
    print(f"File not found: {file_path}")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

