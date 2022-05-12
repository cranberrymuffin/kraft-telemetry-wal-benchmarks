from ruamel.yaml import YAML
import copy
import socket
import uuid
import base64


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
        quorumVoterVal += f'{i + 1}@kafka-{i + 1}:9093,'
    return quorumVoterVal[:len(quorumVoterVal) - 1]


def generateConfig(numBrokers, numControllers):
    yaml = YAML()
    with open('kraft-docker-compose-template-DO-NOT-CHANGE.yaml', 'r') as template:
        example_config = yaml.load(template)
        model_broker = copy.deepcopy(example_config['services']['kafka'])
        del example_config['services']['kafka']

    numNodes = numControllers + numBrokers
    isCombinedRole = numControllers == 0 or numBrokers == 0

    # set cluster wide values
    model_broker['environment']['KAFKA_CONTROLLER_QUORUM_VOTERS'] = \
        getQuorumNodesConfigVal(numNodes if isCombinedRole else numControllers)
    model_broker['environment']['CLUSTER_ID'] = getRandomClusterId()

    ports = getFreePorts(numNodes)

    for i in range(numNodes):
        nodeId = i+1
        new_mode_broker = copy.deepcopy(model_broker)
        new_mode_broker['ports'] = [f'{ports[i * 2]}:9092', f'{ports[i * 2 + 1]}:9093']
        new_mode_broker['environment']['BROKER_ID'] = i + 1
        if not isCombinedRole:
            if i < numControllers:
                new_mode_broker['environment']['NODE_ROLE'] = 'controller'
            else:
                new_mode_broker['environment']['NODE_ROLE'] = 'broker'

        example_config['services'][f'kafka-{nodeId}'] = new_mode_broker

    with open('kraft-docker-compose-autogen.yml', 'w') as template:
        yaml.dump(example_config, template)


if __name__ == '__main__':
    # minimum of 3 controllers
    # 10 100 or 1000 brokers
    generateConfig(3, 3)
