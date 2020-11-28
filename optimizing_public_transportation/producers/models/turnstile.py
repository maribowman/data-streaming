"""Creates a turnstile data producer"""
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

from confluent_kafka import avro
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


@dataclass
class TurnstileEvent:
    station_id: int
    station_name: str
    line: str


class Turnstile(Producer):
    """Defines a turnstile"""
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
                .replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
        )

        super().__init__(
            f"kafka.cta.stations.{station_name}.turnstile",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        for _ in range(self.turnstile_hardware.get_entries(timestamp, time_step)):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={asdict(TurnstileEvent(station_id=self.station.station_id,
                                             station_name=self.station.name,
                                             line=self.station.color.name)
                              )

                       },
                value_schema=Turnstile.value_schema
            )