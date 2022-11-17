from sam.base import messageAgent as ma

ATTR_ALL_ZONE_DETECTION_DICT = 'allZoneDetectionDict'
ATTR_FAILURE = 'failure'
ATTR_ABNORMAL = 'abnormal'
ATTR_SWITCH_ID_LIST = 'switchIDList'
ATTR_SERVER_ID_LIST = 'serverIDList'
ATTR_LINK_ID_LIST = 'linkIDList'

ATTR_QUERY_TYPE = 'query_type'
ATTR_METRIC_NAME = 'metric_name'
ATTR_ZONE = 'zone'
ATTR_TIME_WINDOW = 'time_window_datapoints_num'
ATTR_INSTANCE_TYPE = 'instance_type'
ATTR_INSTANCE_ID_LIST = 'instance_id_list'

INSTANCE_TYPE_SWITCH = 'switches'
INSTANCE_TYPE_LINK = 'links'
INSTANCE_TYPE_SERVER = 'servers'
INSTANCE_TYPE_SFCI = 'sfcis'
INSTANCE_TYPE_VNFI = 'vnfis'

QUERY_TYPE_HISTORY = 'history_value'
QUERY_TYPE_ANOMALY = 'anomaly_record'
QUERY_TYPE_FAILURE = 'failure_record'
QUERY_TYPE_INSTANCE_ID = 'instance_id_list'

ATTR_ACTIVE = 'Active'
ATTR_TIMESTAMP = 'timestamp'
ATTR_SERVER = 'server'
ATTR_SWITCH = 'switch'
ATTR_LINK = 'link'
ATTR_VNFI = 'vnfi'
ATTR_SFCI = 'sfci'
ATTR_VALUE = 'value'
ATTR_HISTORY_VALUE = 'history_value'
ATTR_METRICS = 'metrics'
ATTR_ABNORMAL_STATE = 'abnormal_state'
ATTR_FAILURE_STATE = 'failure_state'
ATTR_LAST_ABNORMAL = 'last_abnormal'
ATTR_LAST_FAILURE = 'last_failure'
ATTR_ID = 'id'

ATTR_SERVER_CPU_UTILIZATION = 'cpu_utilization'
ATTR_SERVER_MEMORY_UTILIZATION = 'memory_utilization'
ATTR_LINK_SYN_RATIO = 'syn_ratio'
ATTR_LINK_DNS_RATIO = 'dns_ratio'

INSTANCE_TYPES = [
    INSTANCE_TYPE_SWITCH,
    INSTANCE_TYPE_SERVER,
    INSTANCE_TYPE_VNFI,
    INSTANCE_TYPE_SFCI,
    INSTANCE_TYPE_LINK,
]
ZONES = [
    ma.SIMULATOR_ZONE,
    ma.TURBONET_ZONE
]
