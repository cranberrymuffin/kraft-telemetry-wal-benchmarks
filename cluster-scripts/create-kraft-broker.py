from ruamel.yaml import YAML
import copy
import socket
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


def getQuorumNodesConfigVal(numNodes):
    quorumVoterVal = ""
    for i in range(numNodes):
        quorumVoterVal += f'{i + 1}@kafka-{i + 1}:29093,'
    return quorumVoterVal[:len(quorumVoterVal) - 1]


def generateConfig(numBrokers, numControllers):
    yaml = YAML()
    yaml.preserve_quotes = True

    # get the example kafka service configs from the template yml file
    with open('kraft-docker-compose-template-DO-NOT-CHANGE.yaml', 'r') as template:
        example_config = yaml.load(template)
        model_broker = copy.deepcopy(example_config['services']['kafka-1'])
        del example_config['services']['kafka-1']

    # determine the total number of services we are going to be creating
    numServices = numControllers + numBrokers

    # find a list of available ports to use for external cluster connections
    ports = getFreePorts(numServices)

    # create the quorum host list based on specified number of controllers
    model_broker['environment']['KAFKA_CONTROLLER_QUORUM_VOTERS'] = getQuorumNodesConfigVal(numControllers)

    bootstrap_servers = []

    for i in range(numServices):
        # make a deepcopy of the model service + tweak values based on role and node number
        new_mode_broker = copy.deepcopy(model_broker)

        # get and set ports to use for this node
        externalPort = ports[i * 2]
        jmxPort = ports[i * 2 + 1]
        new_mode_broker['ports'][0] = f'{externalPort}:{externalPort}'
        new_mode_broker['ports'][1] = f'{jmxPort}:{jmxPort}'
        new_mode_broker['environment']['KAFKA_JMX_PORT'] = jmxPort

        # set names based on service name
        nodeId = i + 1
        hostName = f'kafka-{nodeId}'
        new_mode_broker['hostname'] = hostName
        new_mode_broker['container_name'] = hostName

        # set node id
        new_mode_broker['environment']['KAFKA_BROKER_ID'] = nodeId
        new_mode_broker['environment']['KAFKA_NODE_ID'] = nodeId

        # set listener names
        new_mode_broker['environment']['KAFKA_ADVERTISED_LISTENERS'] = \
            f'PLAINTEXT://{hostName}:29092,PLAINTEXT_HOST://localhost:{externalPort}'
        new_mode_broker['environment']['KAFKA_LISTENERS'] = \
            f'PLAINTEXT://{hostName}:29092,CONTROLLER://{hostName}:29093,PLAINTEXT_HOST://0.0.0.0:{externalPort}'

        # configs for pure brokers (without embedded controllers)
        if i >= numControllers:
            new_mode_broker['environment']['KAFKA_PROCESS_ROLES'] = 'broker'
            # remove controller from kafka listeners
            new_mode_broker['environment']['KAFKA_LISTENERS'] = \
                f'PLAINTEXT://{hostName}:29092,PLAINTEXT_HOST://0.0.0.0:{externalPort}'

        bootstrap_servers.append(f'localhost:{externalPort}')

        # add service to new yaml
        example_config['services'][f'kafka-{nodeId}'] = new_mode_broker

    # dump new yaml
    with open('kraft-docker-compose-autogen.yml', 'w') as template:
        yaml.dump(example_config, template)

    bootstrap_servers = ','.join(bootstrap_servers)
    # TODO: write list to config file in client-pubsub directory
    with open('../benchmark-scripts/producer.config', 'w') as producer_config:
        producer_config.write(f"bootstrap.servers={bootstrap_servers}")

    print(bootstrap_servers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process cluster configurations')
    parser.add_argument('-b', type=int, default=0, help='number of brokers')
    parser.add_argument('-c', type=int, default=0, help='number of controllers')

    args = parser.parse_args()

    if args.b == 0 and args.c == 0:
        raise Exception("must specify at least one broker or controller")

    if args.b > 0 and 0 < args.c < 3:
        raise Exception("must have at least 3 embedded controllers in a mixed cluster")

    generateConfig(args.b, args.c)
