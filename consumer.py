#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka 0.9 balanced Consumer
#
import argparse

from confluent_kafka import Consumer, KafkaException
import sys

import json
import logging
from pprint import pformat


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


def sasl_conf(args):
    sasl_mechanism = args.sasl_mechanism.upper()

    sasl_conf = {'sasl.mechanism': sasl_mechanism,
                 'security.protocol': 'SASL_SSL'}

    sasl_conf.update({'sasl.username': args.user_principal,
                        'sasl.password': args.user_secret})

    return sasl_conf


def main(args):

    broker = args.bootstrap_servers
    group = 'None'
    topics = [args.topic]
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
    #        'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False}
    conf = {'bootstrap.servers': broker, 'group.id': group, 'auto.offset.reset': 'earliest'}
    conf.update(sasl_conf(args))

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    #c.poll(1)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)
    
    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(1)#timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                #sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                #                 (msg.topic(), msg.partition(), msg.offset(),
                #                  str(msg.key())))
                #print(msg.value())
                try:
                    if(msg.key()):
                        print('Message consumed: topic={0}, partition={1}, offset={2}, key={3}, value={4}'.format(
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            msg.key().decode('utf-8'),
                            msg.value().decode('utf-8')))
                    else:
                        print('Message consumed: topic={0}, partition={1}, offset={2}, key={3}, value={4}'.format(
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            msg.key(),#.decode('utf-8'),
                            msg.value().decode('utf-8')))
                except AttributeError as e:
                    #print('Error')
                    print(e)
                # Store the offset associated with msg to a local cache.
                # Stored offsets are committed to Kafka by a background thread every 'auto.commit.interval.ms'.
                # Explicitly storing offsets after processing gives at-least once semantics.
                #c.store_offsets(msg)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        c.unsubscribe()
        # Close down consumer to commit final offsets.
        c.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SASL Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="example_producer_sasl",
                        help="Topic name")
    parser.add_argument('-d', dest="delimiter", default="|",
                        help="Key-Value delimiter. Defaults to '|'"),
    parser.add_argument('-m', dest="sasl_mechanism", default='PLAIN',
                        choices=['GSSAPI', 'PLAIN',
                                 'SCRAM-SHA-512', 'SCRAM-SHA-256'],
                        help="SASL mechanism to use for authentication."
                             "Defaults to PLAIN")
    parser.add_argument('--tls', dest="enab_tls", default=False)
    parser.add_argument('-u', dest="user_principal", required=True,
                        help="Username")
    parser.add_argument('-s', dest="user_secret", required=True,
                        help="Password for PLAIN and SCRAM, or path to"
                             " keytab (ignored on Windows) if GSSAPI.")

    main(parser.parse_args())