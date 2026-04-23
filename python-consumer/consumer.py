import json
import logging
import signal
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import TopicPartition
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger(__name__)
@dataclass
class ConsumerConfig:
    bootstrap_servers: str = 'localhost:9092'
    topic: str = 'test-topic'
    group_id: str = 'my-group'
    auto_offset_reset: str = 'earliest'
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000

class MessageProcessor:
    @staticmethod
    def process_message(message_value: str, partition: int, offset: int) -> None:
        try:
            data = json.loads(message_value)
            logger.info(f"Received JSON message:")
            logger.info(f"  ID: {data.get('id')}")
            logger.info(f"  Data: {data.get('data')}")
            logger.info(f"  Timestamp: {data.get('timestamp')}")
            logger.info(f"  Source: {data.get('source')}")
        except json.JSONDecodeError:
            logger.info(f"Received text message: {message_value}")
        logger.info(f"Partition: {partition}, Offset: {offset}")
        logger.info("-" * 50)
    @staticmethod
    def save_to_file(message: str, partition: int, offset: int) -> None:
        with open('received_messages.log', 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().isoformat()}] Partition: {partition}, Offset: {offset} -> {message}\n")

class KafkaConsumerManager:
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True
        self.message_count = 0
        
    def setup_consumer(self) -> None:
        try:
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers.split(','),
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                max_poll_records=self.config.max_poll_records,
                session_timeout_ms=self.config.session_timeout_ms,
                heartbeat_interval_ms=self.config.heartbeat_interval_ms,
                max_poll_interval_ms=self.config.max_poll_interval_ms,
                value_deserializer=lambda x: x.decode('utf-8') if x else '',
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=1000
            )
            logger.info(f"Consumer created successfully for topic: {self.config.topic}")
            partitions = self.consumer.partitions_for_topic(self.config.topic)
            if partitions:
                logger.info(f"Topic has partitions: {partitions}")
            for partition in partitions:
                tp = TopicPartition(self.config.topic, partition)
                beginning_offset = self.consumer.beginning_offsets([tp])[tp]
                end_offset = self.consumer.end_offsets([tp])[tp]
                logger.info(f"  Partition {partition}: offsets {beginning_offset} -> {end_offset}")
        except KafkaError as e:
            logger.error(f"Failed to create consumer: {e}")
            raise
    
    def signal_handler(self, signum: int, frame) -> None:
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run(self) -> None:
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        if not self.consumer:
            self.setup_consumer()
        logger.info(f"Starting consumer... Waiting for messages")
        logger.info("Press Ctrl+C to stop\n")
        try:
            while self.running:
                messages = self.consumer.poll(timeout_ms=1000, max_records=500)
                for topic_partition, records in messages.items():
                    for record in records:
                        self.message_count += 1
                        logger.info(f"\nMessage #{self.message_count}")
                        MessageProcessor.process_message(
                            record.value,
                            record.partition,
                            record.offset
                        )
                if not self.config.enable_auto_commit:
                    self.consumer.commit()      
        except KeyboardInterrupt:
            logger.info("\nKeyboard interrupt received")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self) -> None:
        logger.info(f"\nTotal messages processed: {self.message_count}")
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
        logger.info("Consumer stopped successfully")

def main():
    import os
    config = ConsumerConfig(
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'localhost:9092'),
        topic=os.getenv('KAFKA_TOPIC', 'test-topic'),
        group_id=os.getenv('KAFKA_GROUP_ID', 'my-group')
    )
    manager = KafkaConsumerManager(config)
    manager.run()

if __name__ == "__main__":
    main()
