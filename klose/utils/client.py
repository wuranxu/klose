"""grpc client
"""
import asyncio
import random
from typing import Any

from grpc_requests.aio import AsyncClient
from loguru import logger

from klose.config import Config
from klose.excpetions.RpcError import RpcError
from klose.utils.etcd import EtcdClient


class RpcClient(object):
    etcd_client = None

    @staticmethod
    def get_key_end(key):
        end = list(key)
        for k in range(len(key) - 1, -1, -1):
            if ord(key[k]) < 255:
                end[k] = chr(ord(key[k]) + 1)
                return "".join(end)
        return "".join(end)

    @staticmethod
    async def get_instance(service):
        """
        获取服务实例，通过grpc-requests
        :return:
        """
        if RpcClient.etcd_client is None:
            RpcClient.etcd_client = EtcdClient(Config.ETCD_ENDPOINT, loop=asyncio.get_event_loop())
        range_end = RpcClient.get_key_end(service)
        addr_list = await RpcClient.etcd_client.list_server(service, range_end)
        random.shuffle(addr_list)
        for addr in addr_list:
            srv = None
            try:
                ins = AsyncClient(addr)
                srv = await ins.service(service)
                result = await srv.HealthCheck()
                assert result.get("state") == "working"
                return srv
            except AttributeError:
                # 说明有这个服务但是没这个方法，直接return
                return srv
            except:
                logger.info(f"服务地址: {addr}可能已经宕机, 尝试更换节点~")
                continue
        raise Exception(f"no available {service} service")

    @staticmethod
    async def invoke(client, method: str, args: Any):
        """
        调用client的参数并返回结果
        :param client:
        :param method:
        :param args:
        :return:
        """
        md = getattr(client, method, None)
        if md is None:
            raise RpcError(f"{method}方法不存在")
        resp = await md(args)
        if resp.get("code", 0) != 0:
            raise RpcError(f"请求{method}方法出错: {resp.get('msg', 'unknown')}")
        return resp.get("data")
