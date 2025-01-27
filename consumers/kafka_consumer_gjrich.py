"""
kafka_consumer_gjrich.py

Consume messages from a Kafka topic and process them with threshold alerts.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Define a function to process a single message
#####################################

def process_message(message: str, initial_price: float, last_alerted_threshold: dict) -> None:
    """
    Process a single message and alert if stock price crosses thresholds.

    Args:
        message (str): The message to process.
        initial_price (float): The initial stock price to calculate percentage changes.
        last_alerted_threshold (dict): A dictionary to track the last alerted threshold.
    """
    # Extract the stock price from the message
    try:
        stock_price = float(message.split("$")[-1])
    except (IndexError, ValueError):
        logger.error(f"Failed to parse stock price from message: {message}")
        return

    # Calculate percentage change
    percentage_change = ((stock_price - initial_price) / initial_price) * 100

    # Define thresholds and corresponding alerts
    thresholds = {
        30: "🚨 ALERT: Stock price has risen by over 30%! 🚨",
        20: "🚨 ALERT: Stock price has risen by over 20%! 🚨",
        10: "🚨 ALERT: Stock price has risen by over 10%! 🚨",
        -10: "🚨 ALERT: Stock price has fallen by over 10%! 🚨",
        -20: "🚨 ALERT: Stock price has fallen by over 20%! 🚨",
        -30: "🚨 ALERT: Stock price has fallen by over 30%! 🚨",
    }

    # Determine the highest threshold crossed
    highest_threshold_crossed = None
    for threshold in sorted(thresholds.keys(), reverse=True):
        if (percentage_change >= threshold and threshold > 0) or (
            percentage_change <= threshold and threshold < 0
        ):
            highest_threshold_crossed = threshold
            break

    # Alert only if the highest threshold crossed is higher than the last alerted threshold
    if highest_threshold_crossed is not None:
        if (
            last_alerted_threshold["value"] is None
            or abs(highest_threshold_crossed) > abs(last_alerted_threshold["value"])
        ):
            logger.warning(
                f"{thresholds[highest_threshold_crossed]} Current price: ${stock_price:.2f}"
            )
            last_alerted_threshold["value"] = highest_threshold_crossed

    # Log the message for debugging
    logger.info(f"Processing message: {message}")
    

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Dynamically captures the initial stock price from the first message.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        # Flag to track if the initial price has been captured
        initial_price_captured = False
        initial_price = None

        # Track the last alerted threshold
        last_alerted_threshold = {"value": None}

        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")

            # Capture the initial price from the first message
            if not initial_price_captured:
                try:
                    # Extract the initial price from the first message
                    initial_price = float(message_str.split("$")[-1])
                    logger.info(f"Initial stock price captured: ${initial_price:.2f}")
                    initial_price_captured = True
                except (IndexError, ValueError):
                    logger.error(f"Failed to capture initial price from message: {message_str}")
                    continue

            # Process the message with the captured initial price
            process_message(message_str, initial_price, last_alerted_threshold)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")
    

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()