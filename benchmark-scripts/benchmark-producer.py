import argparse
import os
import subprocess
import base64
import uuid
import pathlib
import re
from confluent_kafka.admin import AdminClient, NewTopic
from bayes_opt import BayesianOptimization


def getRandomId():
    clusterId = uuid.uuid4().__str__().replace('-', '').encode('ascii')
    clusterId = base64.b64encode(clusterId)
    clusterId = clusterId.decode('ascii')
    return clusterId[:22]


def dir_path(string):
    if os.path.exists(string) and not os.path.isdir(string):
        return string
    else:
        raise FileNotFoundError(string)


def setDefaultConfigs():
    with open('../benchmark-scripts/producer.config', 'a') as producer_config:
        producer_config.write(f"client.id={getRandomId()}")


class ProducerBenchmarker:
    def __init__(self, data_file, delimiter, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.data_path = data_file
        self.delimiter = delimiter
        self.admin_client = AdminClient(
            {'bootstrap.servers': bootstrap_servers,
             'client.id': 'admin_client'}
        )

        pbounds = {
            'compression_type': (0, 5),
            'partitioner_class': (0, 3),
            'acks': (0, 3),
            'enable_idempotence': (0, 2),
            'buffer_memory': (10000, 245618),  # TODO: relative to machine and data file
            'retries': (0, 2147483647),
            'batch_size': (10000, 2147483647),
            'linger_ms': (0, 180000),
            'max_block_ms': (0, 180000),
            'max_request_size': (5000, 245618),  # TODO: relative to machine and data file
            'receive_buffer_bytes': (-2, 2147483647),
            'request_timeout_ms': (0, 2147483647),
            'send_buffer_bytes': (-2, 2147483647),
            'max_in_flight_requests_per_connection': (1, 2147483647),
            'metadata_max_age_ms': (0, 180000),
            'metadata_max_idle_ms': (0, 180000),
        }

        optimizer = BayesianOptimization(
            f=self.benchmark,
            pbounds=pbounds,
            random_state=1,
        )

        optimizer.maximize(init_points=1000,
                           n_iter=500
                           )

    @staticmethod
    def processMetricResults(output):
        recordsPerSecond = ""
        mbPerSecond = ""

        for line in output.split('\n'):
            if line.count("records/sec") == 0:
                continue
            else:
                datapoints = line.split(",")
                throughout = datapoints[1]
                decimalRe = '\d*\.?\d+'
                results = re.findall(decimalRe, throughout)
                recordsPerSecond = results[0]
                mbPerSecond = results[1]

        return recordsPerSecond, mbPerSecond

    def getAndCreateTopic(self):
        topic = getRandomId()
        self.admin_client.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        return topic

    def getBenchmarkCmdOld(self):
        data_path = pathlib.Path(self.data_path).resolve()
        num_lines = sum(1 for _ in open(data_path, 'r'))
        topic = self.getAndCreateTopic()

        benchmarkCmd = f"kafka-producer-perf-test --num-records {num_lines} --throughput -1 " \
                       f"--producer-props bootstrap.servers={self.bootstrap_servers} " \
                       f"--print-metrics --payload-file {data_path} --topic {topic}"
        if self.delimiter != '\n':
            benchmarkCmd += f" --payload-delimiter {self.delimiter}"

        return benchmarkCmd

    def getBenchMarkCmd(self, compression_type, partitioner_class, acks, enable_idempotence,
                        buffer_memory, retries, batch_size, linger_ms, max_block_ms,
                        max_request_size, receive_buffer_bytes, request_timeout_ms, send_buffer_bytes,
                        max_in_flight_requests_per_connection, metadata_max_age_ms, metadata_max_idle_ms):
        compression_type = ['none', 'gzip', 'snappy', 'lz4', 'zstd'][int(compression_type)]
        partitioner_class = ['org.apache.kafka.clients.producer.internals.DefaultPartitioner',
                             'org.apache.kafka.clients.producer.RoundRobinPartitioner',
                             'org.apache.kafka.clients.producer.UniformStickyPartitioner'][int(partitioner_class)]
        acks = 'all'  # [0, 1, 'all'][int(acks)]
        enable_idempotence = ['true', 'false'][int(enable_idempotence)]
        buffer_memory = int(buffer_memory)
        retries = int(retries)
        max_request_size = min(buffer_memory, int(max_request_size))
        batch_size = min(buffer_memory, int(batch_size))
        linger_ms = int(linger_ms)
        max_block_ms = int(max_block_ms)
        receive_buffer_bytes = min(buffer_memory, int(receive_buffer_bytes))
        request_timeout_ms = int(request_timeout_ms)
        delivery_timeout_ms = linger_ms + request_timeout_ms
        send_buffer_bytes = min(buffer_memory, int(send_buffer_bytes))
        max_in_flight_requests_per_connection = int(max_in_flight_requests_per_connection)
        if enable_idempotence == 'true':
            max_in_flight_requests_per_connection = min(5, int(max_in_flight_requests_per_connection))
        metadata_max_age_ms = int(metadata_max_age_ms)
        metadata_max_idle_ms = int(metadata_max_idle_ms)
        data_path = pathlib.Path(self.data_path).resolve()
        num_lines = sum(1 for _ in open(data_path, 'r'))
        benchmarkCmd = f"kafka-producer-perf-test --num-records {num_lines} --payload-file {data_path} --throughput -1 " \
                       f"--print-metrics --topic {getRandomId()} " \
                       f"--producer-props bootstrap.servers={self.bootstrap_servers} client.id={getRandomId()} " \
                       f"compression.type={compression_type} partitioner.class={partitioner_class} " \
                       f"acks={acks} enable.idempotence={enable_idempotence} buffer.memory={buffer_memory} " \
                       f"retries={retries} batch.size={batch_size} delivery.timeout.ms={delivery_timeout_ms} " \
                       f"linger.ms={linger_ms} max.block.ms={max_block_ms} max.request.size={max_request_size} " \
                       f"receive.buffer.bytes={receive_buffer_bytes} request.timeout.ms={request_timeout_ms} " \
                       f"send.buffer.bytes={send_buffer_bytes} " \
                       f"max.in.flight.requests.per.connection={max_in_flight_requests_per_connection} " \
                       f"metadata.max.age.ms={metadata_max_age_ms} metadata.max.idle.ms={metadata_max_idle_ms}"
        if self.delimiter != '\n':
            benchmarkCmd += f" --payload-delimiter {self.delimiter}"

        return benchmarkCmd

    def benchmark(self, compression_type, partitioner_class, acks, enable_idempotence,
                        buffer_memory, retries, batch_size, linger_ms, max_block_ms,
                        max_request_size, receive_buffer_bytes, request_timeout_ms, send_buffer_bytes,
                        max_in_flight_requests_per_connection, metadata_max_age_ms, metadata_max_idle_ms):
        benchmarkCmd = self.getBenchmarkCmd()
        result = subprocess.run(benchmarkCmd.split(' '), capture_output=True, text=True)
        recPerSec, mbPerSec = self.processMetricResults(result.stdout)
        if result.stderr != '':
            raise Exception(result.stderr)

        return -recPerSec


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process producer benchmarking configurations')
    parser.add_argument('-data_file', type=dir_path, help='data file')
    parser.add_argument('-delimiter', type=str, default='\n', help='data file delimiter')
    parser.add_argument('-bootstrap_servers', type=str)
    args = parser.parse_args()

