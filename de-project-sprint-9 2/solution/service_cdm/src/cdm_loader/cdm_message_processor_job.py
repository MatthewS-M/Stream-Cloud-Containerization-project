from datetime import datetime
from logging import Logger
from uuid import UUID
from repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger       






    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        try:
            for _ in range(self._batch_size):
                message = self._consumer.consume()
                if not message:
                    break
                self._cdm_repository.save_message(message)
        except Exception as e:
            self._logger.error(f"An error occurred: {e}")
        finally:
            self._logger.info(f"{datetime.utcnow()}: FINISH")