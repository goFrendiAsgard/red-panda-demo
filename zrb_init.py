from typing import List
from zrb import (
    runner, CmdTask, DockerComposeTask, Task, HTTPChecker, PortChecker,
    Env, EnvFile, ChoiceInput, IntInput, StrInput,
)
import os

KAFKA_BROKERS = 'localhost:19092'
CONSUMER_COUNT_PER_GROUP = 3
CONSUMER_GROUPS = ['warehouse', 'finance']
PRODUCER_KEYS = ['web', 'app']
TOPIC = 'payment'
TOPIC_PARTITION = 3
TOPIC_REPLICATION = 1

current_dir = os.path.dirname(__file__)
client_dir = os.path.join(current_dir, 'src', 'go-client')
client_env_path = os.path.join(client_dir, 'template.env')


remove_redpanda = DockerComposeTask(
    name='stop-redpanda',
    cwd=os.path.join(current_dir, 'src', 'redpanda'),
    compose_cmd='down'
)

start_redpanda = DockerComposeTask(
    name='start-redpanda',
    upstreams=[remove_redpanda],
    cwd=os.path.join(current_dir, 'src', 'redpanda'),
    compose_cmd='up',
    checkers=[
        PortChecker(port=18081, timeout=5),   # redpanda-0
        PortChecker(port=18082, timeout=5),   # redpanda-0
        PortChecker(port=19092, timeout=5),   # redpanda-0
        PortChecker(port=19644, timeout=5),   # redpanda-0
        PortChecker(port=28081, timeout=5),   # redpanda-1
        PortChecker(port=28082, timeout=5),   # redpanda-1
        PortChecker(port=29092, timeout=5),   # redpanda-1
        PortChecker(port=29644, timeout=5),   # redpanda-1
        PortChecker(port=38081, timeout=5),   # redpanda-3
        PortChecker(port=38082, timeout=5),   # redpanda-3
        PortChecker(port=39092, timeout=5),   # redpanda-3
        PortChecker(port=39644, timeout=5),   # redpanda-3
        HTTPChecker(port=8080, method='GET')  # console
    ]
)
runner.register(start_redpanda)


# Consumers
start_consumers: List[Task] = []
for consumer_group in CONSUMER_GROUPS:
    for consumer_index in range(CONSUMER_COUNT_PER_GROUP):
        start_consumer = CmdTask(
            name=f'{consumer_group}-consumer-{consumer_index}',
            cwd=client_dir,
            upstreams=[start_redpanda],
            cmd='go run main.go',
            env_files=[
                EnvFile(env_file=client_env_path)
            ],
            envs=[
                Env(name='APP_MODE', default='consumer'),
                Env(name='APP_KAFKA_BROKERS', default=KAFKA_BROKERS),
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
                CmdTask(
                    name=f'check-{consumer_group}-consumer-{consumer_index}',
                    cmd='sleep 1 && echo "should be ok (hopefully)"'
                )
            ]
        )
        start_consumers.append(start_consumer)

start_demo = CmdTask(
    name='start-demo',
    cmd='echo Ok',
    upstreams=start_consumers
)
runner.register(start_demo)

produce = CmdTask(
    name='produce',
    cwd=client_dir,
    cmd='go run main.go',
    inputs=[
        IntInput(
            name='message-count',
            shortcut='c',
            prompt='How many messages?',
            default=1
        ),
        StrInput(
            name='message',
            shortcut='m',
            prompt='The messages',
            default='Cuan'
        ),
        ChoiceInput(
            name='message-key',
            shortcut='k',
            prompt='Web or app?',
            default='web',
            choices=['web', 'app']
        )
    ],
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
        Env(name='APP_MESSAGE_VALUE', default='{{input.message}}'),
        Env(name='APP_MESSAGE_KEY', default='{{input.message_key}}'),
        Env(name='APP_MESSAGE_REPEAT', default='{{input.message_count}}'),
    ]
)
runner.register(produce)
