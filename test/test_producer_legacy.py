# -*- coding: utf-8 -*-

import os
import collections
import logging
import threading
import time
from datetime import datetime as dtime

from mock import MagicMock, patch
from test import unittest
from test.testutil import DisableUnsentStoringMixin

from kafka import SimpleClient, SimpleProducer, KeyedProducer
from kafka.errors import (
    AsyncProducerQueueFull, FailedPayloadsError, NotLeaderForPartitionError)
from kafka.producer.base import (
    Producer, _send_upstream, KAFKA_UNSENT_FILE, _get_pending_messages)
from kafka.protocol import CODEC_NONE
from kafka.structs import (
    ProduceResponsePayload, RetryOptions, TopicPartition)

from six.moves import queue, xrange


class ProccessRunnerMixin(object):
    def setUp(self):
        super(ProccessRunnerMixin, self).setUp()
        self.client = MagicMock()
        self.queue = queue.Queue()

    def tearDown(self):
        super(ProccessRunnerMixin, self).tearDown()
        # self.thread.join()
        for _ in xrange(self.queue.qsize()):
            self.queue.get()

    def _run_process(
            self, retries_limit=3, sleep_timeout=1, backoff_ms=50,
            batch_length=3):
        # run _send_upstream process with the queue
        start = dtime.now()
        stop_event = threading.Event()
        retry_options = RetryOptions(limit=retries_limit,
                                     backoff_ms=backoff_ms,
                                     retry_on_timeouts=False)
        self.thread = threading.Thread(
            target=_send_upstream,
            args=(self.queue, self.client, CODEC_NONE,
                  0.3,  # batch time (seconds)
                  batch_length,  # batch length
                  Producer.ACK_AFTER_LOCAL_WRITE,
                  Producer.DEFAULT_ACK_TIMEOUT,
                  retry_options,
                  stop_event))
        self.thread.daemon = True
        self.thread.start()
        time.sleep(sleep_timeout)
        stop_event.set()


class TestKafkaProducer(DisableUnsentStoringMixin, unittest.TestCase):
    def test_producer_message_types(self):

        producer = Producer(MagicMock())
        topic = b"test-topic"
        partition = 0

        bad_data_types = (u'你怎么样?', 12, ['a', 'list'],
                          ('a', 'tuple'), {'a': 'dict'}, None,)
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, partition, m)

        good_data_types = (b'a string!',)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, partition, m)

    def test_keyedproducer_message_types(self):
        client = MagicMock()
        client.get_partition_ids_for_topic.return_value = [0, 1]
        producer = KeyedProducer(client)
        topic = b"test-topic"
        key = b"testkey"

        bad_data_types = (u'你怎么样?', 12, ['a', 'list'],
                          ('a', 'tuple'), {'a': 'dict'},)
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, key, m)

        good_data_types = (b'a string!', None,)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, key, m)

    def test_topic_message_types(self):
        client = MagicMock()

        def partitions(topic):
            return [0, 1]

        client.get_partition_ids_for_topic = partitions

        producer = SimpleProducer(client, random_start=False)
        topic = b"test-topic"
        producer.send_messages(topic, b'hi')
        assert client.send_produce_request.called

    @patch('kafka.producer.base._send_upstream')
    def test_producer_async_queue_overfilled(self, mock):
        queue_size = 2
        producer = Producer(MagicMock(), async_send=True,
                            async_queue_maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(AsyncProducerQueueFull):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)
        self.assertEqual(producer.queue.qsize(), queue_size)
        for _ in xrange(producer.queue.qsize()):
            producer.queue.get()

    def test_producer_sync_fail_on_error(self):
        error = FailedPayloadsError('failure')
        with patch.object(SimpleClient, 'load_metadata_for_topics'), \
            patch.object(SimpleClient, 'ensure_topic_exists'), \
            patch.object(SimpleClient, 'get_partition_ids_for_topic', return_value=[0, 1]), \
            patch.object(SimpleClient, '_send_broker_aware_request', return_value=[error]):

            client = SimpleClient(MagicMock())
            producer = SimpleProducer(client, async_send=False, sync_fail_on_error=False)

            # This should not raise
            (response,) = producer.send_messages('foobar', b'test message')
            self.assertEqual(response, error)

            producer = SimpleProducer(client, async_send=False, sync_fail_on_error=True)
            with self.assertRaises(FailedPayloadsError):
                producer.send_messages(b'foobar', b'test message')

    def test_cleanup_is_not_called_on_stopped_producer(self):
        producer = Producer(MagicMock(), async_send=True)
        producer.stopped = True
        with patch.object(producer, 'stop') as mocked_stop:
            producer._cleanup_func(producer)
            self.assertEqual(mocked_stop.call_count, 0)

        # I dunno how tests passed previously, but the issue is that the thread is not stopped
        # (we mocked the stop function when `__init__` exits and original `stop` is never called)
        # so we have to stop manually, otherwise it just fucks up other tests
        # it kinda takes longer to finish tests but... what the hell ?
        producer.stopped = False
        producer.stop()

    def test_cleanup_is_called_on_running_producer(self):
        producer = Producer(MagicMock(), async_send=True)
        producer.stopped = False
        with patch.object(producer, 'stop') as mocked_stop:
            producer._cleanup_func(producer)
            self.assertEqual(mocked_stop.call_count, 1)
        producer.stop()


class TestKafkaProducerSendUpstream(
    DisableUnsentStoringMixin, ProccessRunnerMixin, unittest.TestCase):

    def test_wo_retries(self):

        # lets create a queue and add 10 messages for 1 partition
        for i in range(10):
            self.queue.put((TopicPartition("test", 0), "msg %i", "key %i"))

        self._run_process()

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 4 non-void cals:
        # 3 batches of 3 msgs each + 1 batch of 1 message
        self.assertEqual(self.client.send_produce_request.call_count, 4)

    def test_first_send_failed(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i", "key %i"))

        # Mock offsets counter for closure
        offsets = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        self.client.is_first_time = True

        def send_side_effect(reqs, *args, **kwargs):
            if self.client.is_first_time:
                self.client.is_first_time = False
                return [FailedPayloadsError(req) for req in reqs]
            responses = []
            for req in reqs:
                offset = offsets[req.topic][req.partition]
                offsets[req.topic][req.partition] += len(req.messages)
                responses.append(
                    ProduceResponsePayload(req.topic, req.partition, 0, offset)
                )
            return responses

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 5 non-void calls: 1st failed batch of 3 msgs
        # plus 3 batches of 3 msgs each + 1 batch of 1 message
        self.assertEqual(self.client.send_produce_request.call_count, 5)

    def test_with_limited_retries(self):
        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i" % i, "key %i" % i))

        def send_side_effect(reqs, *args, **kwargs):
            return [FailedPayloadsError(req) for req in reqs]

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(3, 3)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 16 non-void calls:
        # 3 initial batches of 3 msgs each + 1 initial batch of 1 msg +
        # 3 retries of the batches above = (1 + 3 retries) * 4 batches = 16
        self.assertEqual(self.client.send_produce_request.call_count, 16)

    def test_async_producer_not_leader(self):

        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i", "key %i"))

        # Mock offsets counter for closure
        offsets = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        self.client.is_first_time = True

        def send_side_effect(reqs, *args, **kwargs):
            if self.client.is_first_time:
                self.client.is_first_time = False
                return [ProduceResponsePayload(req.topic, req.partition,
                                               NotLeaderForPartitionError.errno, -1)
                        for req in reqs]

            responses = []
            for req in reqs:
                offset = offsets[req.topic][req.partition]
                offsets[req.topic][req.partition] += len(req.messages)
                responses.append(
                    ProduceResponsePayload(req.topic, req.partition, 0, offset)
                )
            return responses

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 5 non-void calls: 1st failed batch of 3 msgs
        # + 3 batches of 3 msgs each + 1 batch of 1 msg = 1 + 3 + 1 = 5
        self.assertEqual(self.client.send_produce_request.call_count, 5)


class TestUnsentKafkaMessages(ProccessRunnerMixin, unittest.TestCase):
    def tearDown(self):
        try:
            os.remove(KAFKA_UNSENT_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

    def test_unsent_message_saved_properly(self):
        # single message
        self.queue.put((TopicPartition("test", 0), "msg %i" % 0, "key %i" % 0))

        def send_side_effect(reqs, *args, **kwargs):
            return [FailedPayloadsError(req) for req in reqs]

        self.client.send_produce_request.side_effect = send_side_effect

        # 4 retries * 900ms. 4 should pass (3 is 2700 and 4ths starts before timeout)
        self._run_process(5, 3, backoff_ms=900)
        self.thread.join()

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        pending_msgs = _get_pending_messages()
        self.assertEqual(1, len(pending_msgs))
        self.assertEqual({
            'key': "key 0",
            'topic': "test",
            'partition': 0,
            'msgs': ["msg 0"],
        }, pending_msgs[0])

    def test_unsent_multiple_messages_saved_properly(self):
        # 14 messages
        for i in range(14):
            self.queue.put((TopicPartition("test", i), "msg %i" % i, "key %i" % i))

        self.client.return_good = False

        def send_side_effect(reqs, *args, **kwargs):
            """
            return "good" response every second time
            """
            if self.client.return_good and reqs[0].partition < 12:
                responses = []
                for req in reqs:
                    responses.append(
                        ProduceResponsePayload(req.topic, req.partition, 0, 0)
                    )
                retval = responses
            else:
                retval = [FailedPayloadsError(req) for req in reqs]

            self.client.return_good = not self.client.return_good

            return retval

        self.client.send_produce_request.side_effect = send_side_effect

        # 14 messages, 1 retry
        # 0-2 msgs batch fail - adding 900ms, calls: 1
        # 0-2 msgs succeed, calls: 2
        # 3-5 msgs fail - adding 900ms, total 1800ms, calls: 3
        # 3-5 msgs succeed, calls: 4
        # 6-8 fail - adding 900ms, total 2700ms, calls: 5
        # 6-8 succeed, calls: 6
        # 9-11 fail, total 3600ms, calls: 7
        # 9-11 succeed, calls: 8
        # 12-13 - fail, 4500ms, calls: 9
        # 12-13 - fail again, calls: 10
        # timed out

        backoff_ms = 900
        self._run_process(1, 4, backoff_ms=backoff_ms)
        self.thread.join()

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        pending_msgs = _get_pending_messages()
        self.assertEqual(2, len(pending_msgs))

