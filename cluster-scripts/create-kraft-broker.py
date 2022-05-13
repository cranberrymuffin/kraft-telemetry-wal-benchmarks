from ruamel.yaml import YAML
import copy
import socket
import uuid
import base64
import argparse


def getFreePorts(numNodes):
    sockets = []
    ports = []
    for i in range((numNodes * 2)):
        sockets.insert(i, socket.socket())
        sockets[i].bind(('', 0))
        ports.insert(i, sockets[i].getsockname()[1])

    for sock in sockets:
        sock.close()

    return ports


def getRandomClusterId():
    clusterId = uuid.uuid4().__str__().replace('-', '').encode('ascii')
    clusterId = base64.b64encode(clusterId)
    clusterId = clusterId.decode('ascii')
    return clusterId[:22]


def getQuorumNodesConfigVal(numNodes):
    quorumVoterVal = ""
    for i in range(numNodes):
        quorumVoterVal += f'{i + 1}@kafka-{i + 1}:29093,'
    return quorumVoterVal[:len(quorumVoterVal) - 1]


def generateConfig(numBrokers, numControllers):
    yaml = YAML()
    yaml.preserve_quotes = True
    with open('kraft-docker-compose-template-DO-NOT-CHANGE.yaml', 'r') as template:
        example_config = yaml.load(template)
        model_broker = copy.deepcopy(example_config['services']['kafka-1'])
        del example_config['services']['kafka-1']

    numNodes = numControllers + numBrokers
    isCombinedRole = numControllers == 0 or numBrokers == 0

    # set cluster wide values
    model_broker['environment']['KAFKA_CONTROLLER_QUORUM_VOTERS'] = \
        getQuorumNodesConfigVal(numNodes if isCombinedRole else numControllers)
    # model_broker['environment']['CLUSTER_ID'] = getRandomClusterId()

    ports = getFreePorts(numNodes)

    for i in range(numNodes):
        nodeId = i + 1
        new_mode_broker = copy.deepcopy(model_broker)
        externalPort = ports[i * 2]
        jmxPort = ports[i * 2 + 1]
        hostName = f'kafka-{i + 1}'
        new_mode_broker['hostname'] = hostName
        new_mode_broker['container_name'] = hostName
        new_mode_broker['ports'][0] = f'{externalPort}:{externalPort}'
        new_mode_broker['ports'][1] = f'{jmxPort}:{jmxPort}'
        new_mode_broker['environment']['KAFKA_BROKER_ID'] = i + 1
        new_mode_broker['environment']['KAFKA_NODE_ID'] = i + 1
        new_mode_broker['environment']['KAFKA_ADVERTISED_LISTENERS'] = \
            f'PLAINTEXT://{hostName}:29092,PLAINTEXT_HOST://localhost:{externalPort}'
        new_mode_broker['environment']['KAFKA_LISTENERS'] = \
            f'PLAINTEXT://{hostName}:29092,CONTROLLER://{hostName}:29093,PLAINTEXT_HOST://0.0.0.0:{externalPort}'
        new_mode_broker['environment']['KAFKA_JMX_PORT'] = jmxPort
        if not isCombinedRole and i >= numControllers:
            new_mode_broker['environment']['KAFKA_PROCESS_ROLES'] = 'broker'
            new_mode_broker['environment']['KAFKA_LISTENERS'] =\
                f'PLAINTEXT://{hostName}:29092,PLAINTEXT_HOST://0.0.0.0:{externalPort}'
        example_config['services'][f'kafka-{nodeId}'] = new_mode_broker

    with open('kraft-docker-compose-autogen.yml', 'w') as template:
        yaml.dump(example_config, template)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process cluster configurations')
    parser.add_argument('-b', type=int, default=0, help='number of brokers')
    parser.add_argument('-c', type=int, default=0, help='number of controllers')

    args = parser.parse_args()

    if args.b == 0 and args.c == 0:
        raise Exception("must specify at least one broker or controller")

    if args.b > 0 and args.c > 0 and args.b <3:
        raise Exception("must have at least 3 embedded controllers in a mixed cluster")

    generateConfig(args.b, args.c)
