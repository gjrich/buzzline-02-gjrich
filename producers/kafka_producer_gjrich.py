import os
import sys
import time
import random  # Import the random module
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def generate_messages(producer, topic):
    """
    Generate a stream of buzz messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
    """
    # Initialize ticker symbol and stock price
    ticker_symbol = "$ORCL"  # You can change this to any ticker symbol
    stock_price = 100  # Starting stock price

    # Send an initial message indicating the markets have opened
    initial_message = f"Markets have opened. Initial stock price for {ticker_symbol}: ${stock_price:.2f}"
    producer.send(topic, value=initial_message)
    logger.info(f"Sent initial message to topic '{topic}': {initial_message}")

    # Define the string list without hardcoded stock prices
    string_list: list = [
        f"{ticker_symbol} Status Quo. Current Stock Price: $",  # Placeholder for stock price
        f"{ticker_symbol} Stock price is down 5. Current Stock Price: $",  # Placeholder for stock price
        f"{ticker_symbol} Stock price is up 5. Current Stock Price: $",  # Placeholder for stock price
        f"{ticker_symbol} Stock price has fallen by 5%. Current Stock Price: $",  # Placeholder for stock price
        f"{ticker_symbol} Stock price has risen by 10%! Current Stock Price: $",  # Placeholder for stock price
    ]

    try:
        while True:
            # Randomly select a message template from the list
            message_template = random.choice(string_list)
            
            # Update the stock price based on the selected message
            if "down 5" in message_template:
                stock_price -= 5
            elif "up 5" in message_template:
                stock_price += 5
            elif "fallen by 5%" in message_template:
                stock_price *= 0.95
            elif "risen by 10%" in message_template:
                stock_price *= 1.10

            # Construct the final message with the updated stock price
            message = f"{message_template}{stock_price:.2f}"
            logger.info(f"Generated stock info: {message}")

            # Send the message to the Kafka topic
            producer.send(topic, value=message)
            logger.info(f"Sent message to topic '{topic}': {message}")

            # Sleep for a random interval between 1 and 10 seconds
            interval_secs = random.randint(1, 10)
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated buzz message strings to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic)

    logger.info("END producer.")

if __name__ == "__main__":
    main()