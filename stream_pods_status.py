from kubernetes import client, config
import logging
from logger.logger import KafkaLoggingHandler
from configs.env import KAFKA_PASSWORD, KAFKA_USER, KAFKA_HOST, KAFKA_LOG_TOPIC

def load_conf():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return v1


def stream_qos(func):
    from kubernetes import watch
    watch = watch.Watch()
    logger = logging.getLogger('MyCoolProject')
    kh = KafkaLoggingHandler(KAFKA_HOST, KAFKA_LOG_TOPIC)
    logger.addHandler(kh)
    logger.setLevel(logging.DEBUG)
    try:
        core_v1 = load_conf()
        logging.info("config file load sucsessfully")
    except:
       ...
    def wrapper():
        res = list()
        for ns in func():
            for event in watch.stream(func=core_v1.list_namespaced_pod, namespace=ns, timeout_seconds=60):
                qos_status = event["object"].status.qos_class                  
                container_name = event["object"].status.container_statuses[0].name
                container_ready = event["object"].status.container_statuses[0].ready
                container_phase = event["object"].status.phase
                container_image = event["object"].status.container_statuses[0].image
                print(f"qos_status : {qos_status} container_status: name={container_name}, ready={container_ready}, last_status={container_phase}, image={container_image}  in namespace: {ns}")
                res_per_ns = {"container_name": container_name,
                        "container_image": container_image,
                        "container_status": container_phase,
                        "container_ready": container_ready
                }
                res.append(res_per_ns)
        return res
    return wrapper

@stream_qos
def list_ns():
    v1 = load_conf()
    nameSpaceList = v1.list_namespace()
    for nameSpace in nameSpaceList.items:
        yield nameSpace.metadata.name


if __name__ == "__main__":
    list_ns()
