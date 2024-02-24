from datetime import datetime
from logging import Logger
from cdm_loader.repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect
import json

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
        cdm_repository = CdmRepository(self._cdm_repository, self._logger)

        try:
            for _ in range(self._batch_size):
                message = self._consumer.consume()
                if not message:
                    self._logger.error(f"\n======\n======--- NO MESSAGE WAS CONSUMED ---======\n======\n")
                    break

                self._logger.info(f"\n======\n======\nMESSAGE with structure: \n{message}\n======\n======\n")

                cdm_repository.save_message(message)
                self._logger.info(f"\n======\n======--- MESSAGE WAS PROCESSED ---======\n======\n")
        except Exception as e:
            self._logger.error(f"An error occurred: {e}")
        finally:
            self._logger.info(f"{datetime.utcnow()}: FINISH")