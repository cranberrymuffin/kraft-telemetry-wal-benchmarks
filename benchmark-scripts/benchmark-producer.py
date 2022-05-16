import argparse
import os
import subprocess
import base64
import uuid
import pathlib
import re
from confluent_kafka.admin import AdminClient, NewTopic


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

    # TODO change from topic from hardcoded value to one generated from admin client
    def runBenchmarks(self):
        data_path = pathlib.Path(self.data_path).resolve()
        num_lines = sum(1 for _ in open(data_path, 'r'))
        topic = self.getAndCreateTopic()
        benchmarkCmd = f"kafka-producer-perf-test --num-records {num_lines} --throughput -1 " \
                       f"--producer-props bootstrap.servers={self.bootstrap_servers} "\
                       f"--print-metrics --payload-file {data_path} --topic {topic}"
        if self.delimiter != '\n':
            benchmarkCmd += f" --payload-delimiter {self.delimiter}"
        result = subprocess.run(benchmarkCmd.split(' '), capture_output=True, text=True)
        recPerSec, mbPerSec = self.processMetricResults(result.stdout)
        if result.stderr != '':
            raise Exception(result.stderr)

        print(f"{recPerSec} records/sec; {mbPerSec} mb/sec")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process producer benchmarking configurations')
    parser.add_argument('-data_file', type=dir_path, help='data file')
    parser.add_argument('-delimiter', type=str, default='\n', help='data file delimiter')
    parser.add_argument('-bootstrap_servers', type=str)
    args = parser.parse_args()
    print(args.delimiter)
    ProducerBenchmarker(args.data_file, args.delimiter, args.bootstrap_servers).runBenchmarks()
