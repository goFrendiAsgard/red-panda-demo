from typing import List
from zrb import (
    runner, CmdTask, Env, EnvFile, HTTPChecker, PortChecker, Task
)
import os

CONSUMER_COUNT_PER_GROUP = 3
CONSUMER_GROUPS = ['warehouse', 'finance']
PRODUCER_KEYS = ['web', 'app']
TOPIC = 'payment'
TOPIC_PARTITION = 3
TOPIC_REPLICATION = 2

current_dir = os.path.dirname(__file__)
client_dir = os.path.join(current_dir, 'src', 'go-client')
client_env_path = os.path.join(client_dir, 'template.env')


# Starting docker-compose.
# The docker-compose file is located at src/redpanda/docker-compose.yml
# See: https://docs.redpanda.com/docs/get-started/quick-start/?quickstart=docker&num-brokers=three
start_docker = CmdTask(
    name='start-docker',
    cwd=os.path.join(current_dir, 'src', 'redpanda'),
    cmd=[
        'docker compose down',
        'docker compose up',
    ],
    checkers=[
        PortChecker(port=19092, timeout=5),   # redpanda-0
        PortChecker(port=29092, timeout=5),   # redpanda-1
        PortChecker(port=39092, timeout=5),   # redpanda-2
        HTTPChecker(port=8080, method='GET')  # console
    ]
)
runner.register(start_docker)


# Consumers
start_consumers: List[Task] = []
for consumer_group in CONSUMER_GROUPS:
    for consumer_index in range(CONSUMER_COUNT_PER_GROUP):
        start_consumer = CmdTask(
            name=f'{consumer_group}-consumer-{consumer_index}',
            cwd=client_dir,
            upstreams=[start_docker],
            cmd='go run main.go',
            env_files=[
                EnvFile(env_file=client_env_path)
            ],
            envs=[
                Env(name='APP_MODE', default='consumer'),
                Env(name='APP_KAFKA_BROKERS', default='localhost:19092'),
                Env(name='APP_KAFKA_TOPIC', default=TOPIC),
                Env(
                    name='APP_KAFKA_TOPIC_REPLICATION',
                    default=str(TOPIC_REPLICATION)
                ),
                Env(
                    name='APP_KAFKA_TOPIC_PARTITION',
                    default=str(TOPIC_PARTITION)
                ),
                Env(name='APP_CONSUMER_GROUP', default=consumer_group),
            ],
            checkers=[
                CmdTask(name='check', cmd='echo ok')
            ]
        )
        start_consumers.append(start_consumer)

# Producers
start_producers: List[Task] = []
for producer_key in PRODUCER_KEYS:
    start_producer = CmdTask(
        name=f'{producer_key}-producer',
        cwd=client_dir,
        upstreams=[start_docker],
        cmd='go run main.go',
        env_files=[
            EnvFile(env_file=client_env_path)
        ],
        envs=[
            Env(name='APP_MODE', default='producer'),
            Env(name='APP_KAFKA_BROKERS', default='localhost:19092'),
            Env(name='APP_KAFKA_TOPIC', default=TOPIC),
            Env(
                name='APP_KAFKA_TOPIC_REPLICATION',
                default=str(TOPIC_REPLICATION)
            ),
            Env(
                name='APP_KAFKA_TOPIC_PARTITION',
                default=str(TOPIC_PARTITION)
            ),
            Env(name='APP_MESSAGE_KEY', default=producer_key),
            Env(name='APP_MESSAGE_REPEAT', default='5'),
        ]
    )
    start_producers.append(start_producer)


start_demo = CmdTask(
    name='start-demo',
    cmd='echo Ok',
    upstreams=start_consumers + start_producers
)
runner.register(start_demo)

produce = CmdTask(
    name='produce',
    cwd=client_dir,
    cmd='go run main.go',
    env_files=[
        EnvFile(env_file=client_env_path)
    ],
    envs=[
        Env(name='APP_MODE', default='producer'),
        Env(name='APP_KAFKA_BROKERS', default='localhost:19092'),
        Env(name='APP_KAFKA_TOPIC', default=TOPIC),
        Env(
            name='APP_KAFKA_TOPIC_REPLICATION',
            default=str(TOPIC_REPLICATION)
        ),
        Env(
            name='APP_KAFKA_TOPIC_PARTITION',
            default=str(TOPIC_PARTITION)
        ),
        Env(name='APP_MESSAGE_KEY', default='default'),
        Env(name='APP_MESSAGE_REPEAT', default='5'),
    ]
)
runner.register(produce)
