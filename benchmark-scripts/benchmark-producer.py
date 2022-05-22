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
            'acks': (0, 3),
            'buffer_memory': (10000, 245618),  # TODO: relative to machine and data file
            'batch_size': (10000, 2147483647),
             # TODO: figure this out
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

    def getBenchmarkCmd(self, buffer_memory, compression_type, batch_size, delivery_timeout_ms, acks):
        # TODO figure out relationship between buf memory and batch size
        buffer_memory = int(buffer_memory)
        batch_size = min(buffer_memory, int(batch_size))
        compression_type = ['none', 'gzip', 'snappy', 'lz4', 'zstd'][int(compression_type)]
        # TODO figure out this parameter
        delivery_timeout_ms = linger_ms + request_timeout_ms
        acks = [0, 1, 'all'][int(acks)]

        data_path = pathlib.Path(self.data_path).resolve()
        num_lines = sum(1 for _ in open(data_path, 'r'))
        topic = self.getAndCreateTopic()

        benchmarkCmd = f"kafka-producer-perf-test --num-records {num_lines} --throughput -1 " \
                       f"--producer-props bootstrap.servers={self.bootstrap_servers} " \
                       f"compression.type={compression_type} " \
                       f"batch.size={batch_size} " \
                       f"delivery.timeout.ms={delivery_timeout_ms} " \
                       f"acks={acks} " \
                       f"--print-metrics --payload-file {data_path} --topic {topic}"
        if self.delimiter != '\n':
            benchmarkCmd += f" --payload-delimiter {self.delimiter}"

        return benchmarkCmd

    def benchmark(self, buffer_memory, compression_type, batch_size, delivery_timeout_ms, acks):
        benchmarkCmd = self.getBenchmarkCmd(buffer_memory, compression_type, batch_size, delivery_timeout_ms, acks)
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

