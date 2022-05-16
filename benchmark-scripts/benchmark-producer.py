import argparse
import os
import subprocess
import base64
import uuid
import pathlib
import re


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


# TODO change from topic from hardcoded value to one generated from admin client
def runBenchmarks(data_path, delimiter):
    data_path = pathlib.Path(data_path).resolve()
    producerConfig = pathlib.Path('producer.config').resolve()
    num_lines = sum(1 for _ in open(data_path, 'r'))
    benchmarkCmd = f"kafka-producer-perf-test --num-records {num_lines} --throughput -1 " \
                   f"--producer.config {producerConfig} " \
                   f"--print-metrics --payload-file {data_path} --topic GaoVh9wTS-Gykm5z2GEbPA"
    if delimiter != '\n':
        benchmarkCmd += f" --payload-delimiter {delimiter}"
    result = subprocess.run(benchmarkCmd.split(' '), capture_output=True, text=True)
    recPerSec, mbPerSec = processMetricResults(result.stdout)
    print(f"{recPerSec} records/sec; {mbPerSec} mb/sec")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process producer benchmarking configurations')
    parser.add_argument('-data_file', type=dir_path, help='data file')
    parser.add_argument('-delimiter', type=str, default='\n', help='data file delimiter')

    args = parser.parse_args()

    runBenchmarks(args.data_file, args.delimiter)
