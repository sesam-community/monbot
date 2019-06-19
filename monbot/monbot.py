import datetime
import json
import logging
import os
import time

import requests
import sesamclient
from applicationinsights import TelemetryClient

logger = logging.getLogger('monbot')


def setup_logger():
    """config app logger

    """
    loglevel = os.environ.get("LOGLEVEL", "DEBUG")
    if loglevel.upper() == 'INFO':
        logger.setLevel(logging.INFO)
    elif loglevel.upper() == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif loglevel.upper() == 'WARN':
        logger.setLevel(logging.WARN)
    elif loglevel.upper() == 'ERROR':
        logger.setLevel(logging.ERROR)
    else:
        logger.setlevel(logging.DEBUG)
        logger.WARN(
            "Define an unsupported loglevel. Using the default level: DEBUG.")

    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    # Log handler for console
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    # log handler for file
    # file_handler = logging.FileHandler("./sesambot.log")
    # file_handler.setFormatter(logging.Formatter(format_string))
    # logger.addHandler(file_handler)


def get_whitelist_pipes(whitelist_file='whitelist-prod.txt'):
    with open(whitelist_file, 'r') as input_file:
        pipes = [
            i[6:-11] for i in input_file.readlines() if i.startswith('pipes')
        ]
    return pipes


class NodeInsight():
    def __init__(self, app_setting):
        logger.debug(app_setting)
        node_setting = app_setting['node_setting']
        self._monitoring_list_setting = app_setting['monitoring_list']
        self._api_connection = sesamclient.Connection(
            sesamapi_base_url=node_setting['node_url'],
            jwt_auth_token=node_setting['node_token'],
            timeout=node_setting['timeout'],
            verify_ssl=node_setting['verify_ssl'])
        self._app_setting = app_setting
        self._pipe_offset = {}
        self._pipes_status = None
        self._fresh_pipe_status()
        self._insight_client = None

    def set_monitor_list(self, monitor_list, force=False):
        for pipe in monitor_list:
            if force or (pipe not in self._pipe_offset):
                self._pipe_offset[pipe] = -1

    @property
    def insight_client(self):
        return self._insight_client

    @insight_client.setter
    def insight_client(self, client):
        self._insight_client = client

    def _get_remote_whitelist(self):
        response = requests.get(
            self._monitoring_list_setting['url'],
            auth=(self._monitoring_list_setting['user'],
                  self._monitoring_list_setting['password']))
        response.raise_for_status()
        whitelist = response.text
        pipes = [
            i[6:-10] for i in whitelist.split('\n') if i.startswith('pipes')
        ]
        logger.debug(pipes)

        filtered_pipes = []
        for pipe in pipes:
            parts = pipe.split('-')
            if parts[0] in self._monitoring_list_setting[
                    'pipe_starts_with'] or parts[
                        -1] in self._monitoring_list_setting['pipe_ends_with']:
                filtered_pipes.append(pipe)
        return filtered_pipes

    def exec(self):
        """ exec the insight jobs
        """
        logger.info("InsightNode start at %s. It can be stopped by ctrl+c.",
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        timer = 0
        LOWER = 3  # 15min if internval is 300s
        LOWEST = 48  # 4hrs if interval is 300s
        while True:
            try:
                if timer == 0:
                    pipes = self._get_remote_whitelist()
                    logger.debug(pipes)
                    logger.info(
                        "Monitoring %d pipes based on the whitelist...",
                        len(pipes))
                    self.set_monitor_list(pipes)
                    if not self._pipe_offset:
                        logger.warning(
                            "Monitoring list is empty. Retry to retrieve the whitelist in %s sec..",
                            self._app_setting['interval'])
                        time.sleep(self._app_setting['interval'])
                        continue
                logger.info(
                    "Insighting #%d started at %s...", timer,
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                self.insight_node_api()
                logger.info("Insighting /api/pipes is done...")

                if timer == 0:
                    self.insight_datasets_size()
                    logger.info("Insighting datasets size is done...")

                if timer % LOWER == 0:
                    new_pipe_offsets = {}
                    for sink_id in self._pipe_offset:
                        try:
                            new_pipe_offsets[sink_id] = self.insight_sink(sink_id)
                        except Exception as ex:
                            logger.error("Failed to check the exec log for %s", sink_id)
                            logger.error(ex, exc_info=True)
                    self._pipe_offset = new_pipe_offsets
                    logger.info("Insighting sink log is done...")

                logger.info(
                    "Insighting #%d ended at %s.", timer,
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            except Exception as ex:
                logger.error(ex, exc_info=True)

            timer = 0 if timer == (LOWEST - 1) else (timer + 1)
            time.sleep(self._app_setting['interval'])

    def _get_sink_context(self, sink_id):
        pipe = [i for i in self._pipes_status if i.id == sink_id]
        context = {}
        context['id'] = sink_id
        if pipe:
            if len(pipe) > 1:
                logger.warning("Found duplicated id: %s...", sink_id)
            pipe_node = pipe[0]
            pipe_config = pipe_node.config["effective"]
            context['source_system'] = pipe_config['source'].get(
                'system', 'node')
            context['sink_system'] = pipe_config.get('sink', {}).get(
                'system', 'node')
        return context

    def _fresh_pipe_status(self):
        self._pipes_status = self._api_connection.get_pipes()

    def insight_node_api(self):
        try:
            start_ts = datetime.datetime.now()
            self._fresh_pipe_status()
            end_ts = datetime.datetime.now()
            delta = (end_ts - start_ts).total_seconds()
        except Exception as ex:
            logger.error(ex, exc_info=True)
            delta = 200
        logger.debug("Taking %s s to get the response from api/pipes", delta)
        self._insight_client.track_metric('api_pipes_response_time', delta)

    def insight_datasets_size(self):
        result = self._api_connection.session.get(
            self._api_connection.sesamapi_base_url + "/datasets")
        datasets = result.json()
        user_datasets = [
            ds for ds in datasets if ds['runtime']['origin'] == "user"
        ]
        counter = 0
        for dataset in user_datasets:
            context = self._get_sink_context(dataset['_id'])
            disk_usage = dataset.get('storage', 0) / \
                1024 / 1024  # convert bytes to MB
            counter = counter + 1
            logger.debug("%s used %s MB", dataset['_id'], disk_usage)
            self._insight_client.track_metric(
                name='dataset-size', value=disk_usage, properties=context)
            if counter == 9:
                self._insight_client.flush()
                counter = 0
        self._insight_client.flush()

    def _get_last_seqno(self, sink_id):
        """Get the seqno of the last entity in the exec log

        :sink_id: the sink id
        :returns: the sequence_no of the last entity in the exec log

        """
        pump_ds_id = 'system:pump:' + sink_id
        pump_log_dataset = self._api_connection.get_dataset(pump_ds_id)
        entities = [entity for entity in pump_log_dataset.get_entities(
            history=False, deleted=False, reverse=True, limit=1)]
        offset = entities[0]['_updated'] if entities else -1
        return offset

    def insight_sink(self, sink_id):
        """Create metrics for a pump

        :sink_id: pump id
        :context: the context for the pump
        :returns:

        """
        pump_ds_id = 'system:pump:' + sink_id
        pump_log_dataset = self._api_connection.get_dataset(pump_ds_id)
        if not pump_log_dataset:
            logger.warning("%s not exist..", pump_ds_id)
            return -1
        if not self._app_setting['insight_from_begin'] and self._pipe_offset[
                sink_id] == -1:
            new_offset = self._get_last_seqno(sink_id)
            logger.debug("Init the insight at %d for %s...", new_offset, pump_ds_id)
            return new_offset
        logger.debug("Insighting %s from offset:%d", sink_id,
                     self._pipe_offset[sink_id])
        since = self._pipe_offset[
            sink_id] if self._pipe_offset[sink_id] > 0 else 0
        exec_log = pump_log_dataset.get_entities(
            since=since, history=True, deleted=False, reverse=False)
        metrics = {
            "total_processed_entities": 0,
            "total_changed_entities": 0,
            "min_changed": 0,
            "max_changed": 0,
            "number_of_runs": 0,
            # "ave_time": 0,
            "total_time": 0,
            "min_time": 0,
            "max_time": 0,
            # "last_completed_time": "1900-01-01T00:00:00Z",
            "last_offset": 0
        }
        lastest_completed = None
        new_offset = since
        for entity in exec_log:
            if entity["_id"] == "pump-completed":
                lastest_completed = entity
                metrics["total_processed_entities"] += entity["metrics"][
                    "entities"]["entities_last_run"]
                metrics["total_changed_entities"] += entity["metrics"][
                    "entities"]["changes_last_run"]
                metrics["min_changed"] = entity["metrics"]["entities"][
                    "changes_last_run"] if entity["metrics"]["entities"][
                        "changes_last_run"] < metrics[
                            "min_changed"] else metrics["min_changed"]
                metrics["max_changed"] = entity["metrics"]["entities"][
                    "changes_last_run"] if entity["metrics"]["entities"][
                        "changes_last_run"] > metrics[
                            "max_changed"] else metrics["max_changed"]
                metrics["number_of_runs"] += 1
                metrics["min_time"] = float(
                    entity["total_time"]) if entity["total_time"] < metrics[
                        "min_time"] else metrics["min_time"]
                metrics["max_time"] = float(
                    entity["total_time"]) if entity["total_time"] > metrics[
                        "max_time"] else metrics["max_time"]
                metrics["total_time"] += float(entity["total_time"])
            new_offset = entity["_updated"]

        metrics["total_time"] = round(metrics['total_time'], 6)
        metrics["min_time"] = round(metrics['min_time'], 6)
        metrics["max_time"] = round(metrics['max_time'], 6)

        if not lastest_completed:
            return new_offset

        context = self._get_sink_context(sink_id)
        context['offset'] = new_offset
        logger.debug("%s is metrics: %s", sink_id, metrics)
        self._insight_client.context.operation.id = sink_id
        self._insight_client.track_metric(
            name='sink-performance',
            value=metrics['total_time'],
            type=None,
            count=metrics['number_of_runs'],
            min=metrics['min_time'],
            max=metrics['max_time'],
            std_dev=None,
            properties=context)
        self._insight_client.track_metric(
            name='sink-entity-changed',
            value=metrics['total_changed_entities'],
            type=None,
            count=metrics['number_of_runs'],
            min=metrics['min_changed'],
            max=metrics['max_changed'],
            std_dev=None,
            properties=context)
        self._insight_client.flush()
        logger.debug("Insighting %s completed at offset:%d", sink_id,
                     new_offset)
        return new_offset


class AppInsClient(TelemetryClient):
    def __init__(self, instrumentation_key):
        super().__init__(instrumentation_key=instrumentation_key)

    def test_track_metric(self):
        logger.debug("Running foo_tc_track_metric..")
        self.track_metric('telemetry_test', '42')  # value must be a number
        self.flush()
        logger.debug("Completed foo_tc_track_metric...")

    def test_track_event(self):
        logger.debug("Running foo_tc_track_event...")
        self.track_event('foo', {'telemetry_test': 'foo'}, {'faa': 42})
        self.flush()
        logger.debug("Completed foo_tc_track_event...")


def main():
    with open('app_setting.json', 'r') as conf_file:
        app_setting = json.load(conf_file)
    bm_client = NodeInsight(app_setting)
    bm_client.insight_client = AppInsClient(app_setting['instrumentation_key'])
    bm_client.insight_client.context.operation.parentid = app_setting[
        'node_setting']['node_url']
    bm_client.exec()


if __name__ == "__main__":
    setup_logger()
    main()
