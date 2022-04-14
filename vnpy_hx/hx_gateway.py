import hashlib
import hmac
import json
import sys
import time
from copy import copy
from pytz import timezone
from datetime import datetime
from urllib.parse import urlencode
from typing import Dict, List, Tuple
from types import TracebackType
from threading import Lock

from vnpy.event.engine import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData
)
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Offset,
    OrderType,
    Product,
    Status,
    Interval
)

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient

# 中国时区
CHINA_TZ: timezone = timezone("Asia/Shanghai")

# REST API地址
REST_HOST: str = "https://demotrade.alphazone-data.cn"
# REST_HOST: str = "https://139.224.34.155:52223"

# Websocket API地址
PUBLIC_WEBSOCKET_HOST: str = "wss://demotrade.alphazone-data.cn/ws"

# 商品类型映射
PRODUCTTYPE_VT2HX = {
    Product.FOREX: "FOREX"
}

PRODUCTTYPE_HX2VT = {v: k for k, v in PRODUCTTYPE_VT2HX.items()}

# 数据长度限制映射
INTERVAL_VT2HX: Dict[Interval, int] = {
    Interval.MINUTE: 1,
    Interval.HOUR: 60,
    Interval.DAILY: 1440,
}

# 下单类型
ORDERCMD: List[str] = ["BUY", "SELL", "BUYLIMIT", "SELLLIMIT", "BUYSTOP", "SELLSTOP", "BALANCE", "CREDIT"]

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class HxGateway(BaseGateway):
    """
    VeighNa用于对接火象的交易接口。
    """
    default_name: str = "HX"

    default_setting = {
        "API Key": "",
        "Secret Key": "",
        "服务器": ["Hx-Server"]
    }
    exchanges = [Exchange.HX]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.rest_api: "HxRestApi" = HxRestApi(self)
        self.ws_api: "HxWebsocketApi" = HxWebsocketApi(self)

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["API Key"]
        secret: str = setting["Secret Key"]
        self.rest_api.connect(key, secret, 3)
        self.ws_api.connect(key, secret)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托开仓"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托平仓"""
        self.rest_api.close_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)


class HxRestApi(RestClient):
    """火象restapi接口"""

    def __init__(self, gateway: HxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: HxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.simulated: bool = False

        # 累计开仓量
        self.positions: Dict[str, PositionData] = {}

        # 确保生成的orderid不发生冲突
        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        # 委托映射
        self.comment_ticket_map: Dict[str, str] = {}

    def sign(self, request: Request) -> Request:
        """签名鉴权"""
        timestamp = generate_time()
        request.data = json.dumps(request.data)
        if request.params:
            path: str = request.path + "?" + urlencode(request.params)
        else:
            path: str = request.path
        if len(path) == 0:
            path = "/"
        msg: str = str(timestamp) + path
        msg += request.data
        signature: bytes = generate_signature(msg, self.secret)
        # 添加请求头
        request.headers = {
            "ACCESS-KEY": self.key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": str(timestamp),
            "Content-Type": "application/json"
        }
        return request

    def connect(
        self,
        key: str,
        secret: str,
        session_number: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret
        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(REST_HOST)
        self.start(session_number)
        self.gateway.write_log("REST API启动成功")

        self.query_time()
        self.query_account()
        self.query_order()
        self.query_position()

    def query_time(self) -> None:
        """查询时间"""
        path: str = "/v2/time"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_time
        )

    def query_account(self) -> None:
        """查询账户信息"""
        path: str = "/v2/account_info"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_account,
        )

    def query_order(self) -> None:
        """查询已开仓订单"""
        path: str = "/v2/position"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_order,
        )

    def query_position(self) -> None:
        """查询持仓"""
        path: str = "/v2/deals"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_query_deals,
        )

    def send_order(self, req: OrderRequest) -> str:
        """委托开仓"""
        path: str = "/v2/order/open"

        # 生成委托号
        orderid: str = str(self.connect_time + self._new_order_id())

        # 生成委托
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )

        cmd = 0
        if req.direction == Direction.LONG and req.type == OrderType.MARKET:
            cmd = 0
        elif req.direction == Direction.LONG and req.type == OrderType.LIMIT:
            cmd = 2
        elif req.direction == Direction.SHORT and req.type == OrderType.MARKET:
            cmd = 1
        elif req.direction == Direction.SHORT and req.type == OrderType.LIMIT:
            cmd = 3

        self.add_request(
            method="POST",
            path=path,
            data={
                "lots": req.volume,
                "cmd": cmd,
                "open_price": req.price,
                "symbol": req.symbol,
                "tp": 0,
                "sl": 0,
                "comment": orderid,
            },
            callback=self.on_send_order,
            extra=order
        )

        return order.vt_orderid

    def close_order(self, req: CancelRequest) -> None:
        """委托平仓"""
        path: str = "/v2/order/close"

        data: dict = {
            "lots": 0,
            "symbol": req.symbol
        }

        # 检查是否含comment
        if req.orderid in self.comment_ticket_map:
            data["comment"] = req.orderid   # 如果含comment则使用comment平仓
        else:
            data["ticket"] = req.orderid    # 如果不含comment则使用ticket平仓

        self.add_request(
            method="POST",
            path=path,
            data=data,
            callback=self.on_cancel_order,
        )

    def modify_order(self, data) -> None:
        """修改订单"""
        path: str = "/v2/order/modify"

        self.add_request(
            method="POST",
            path=path,
            data={
                "tp": data["tp"],
                "sl": data["sl"],
                "ticket": data["ticket"],
                "open_price": data["open_price"],
            },
            callback=self.on_modify_order,
        )

    def on_query_time(self, packet: dict, request: Request) -> None:
        """时间查询回报"""
        timestamp: int = int(packet["data"]["ts"])
        server_time: datetime = datetime.fromtimestamp(timestamp, CHINA_TZ)
        local_time: datetime = datetime.now()

        msg: str = f"服务器时间：{server_time}，本机时间：{local_time}"
        self.gateway.write_log(msg)

    def on_query_account(self, packet: dict, request: Request) -> None:
        """账户资金查询回报"""
        if not packet["data"]:
            return

        account: AccountData = AccountData(
            accountid=str(packet["data"]["login"]),
            balance=packet["data"]["equity"],
            frozen=packet["data"]["margin"],
            gateway_name=self.gateway_name,
        )
        self.gateway.on_account(account)
        self.gateway.write_log("资金信息查询成功")

    def on_query_order(self, packet: dict, request: Request) -> None:
        """已开仓订单查询回报"""
        if not packet["data"]:
            return

        for order_info in packet["data"]:
            # 推送已开仓订单
            order: OrderData = self._parse_order_data(order_info, self.gateway_name)
            self.gateway.on_order(order)

            # 合约累计净开仓量
            if order_info["cmd"] < 2:
                volume = order.volume
                if order_info["cmd"] == 1:
                    volume *= -1
                if order.vt_symbol in self.positions:
                    self.positions[order.vt_symbol].volume += volume
                    self.positions[order.vt_symbol].volume = round(self.positions[order.vt_symbol].volume, 2)
                    self.positions[order.vt_symbol].pnl += order_info["profit"]
                else:
                    self.positions[order.vt_symbol] = PositionData(
                        symbol=order.symbol,
                        exchange=order.exchange,
                        direction=Direction.LONG,
                        volume=volume,
                        pnl=order_info["profit"],
                        gateway_name=self.gateway_name,
                    )

        if not self.positions:
            return

        for position in self.positions.values():
            if position.volume < 0:
                position.direction = Direction.SHORT
                position.volume *= -1
            self.gateway.on_position(position)

        self.gateway.write_log("持仓查询成功")

    def on_update_position(self, trade: TradeData) -> None:
        """持仓更新回报"""
        volume = trade.volume
        if trade.direction == Direction.SHORT:
            volume *= -1

        if trade.vt_symbol in self.positions:
            self.positions[trade.vt_symbol].volume += volume
            self.positions[trade.vt_symbol].volume = round(self.positions[trade.vt_symbol].volume, 2)
        else:
            self.positions[trade.vt_symbol] = PositionData(
                symbol=trade.symbol,
                exchange=trade.exchange,
                direction=trade.direction,
                volume=volume,
                pnl=0,
                gateway_name=self.gateway_name,
            )

        for position in self.positions.values():
            if position.volume < 0:
                position.direction = Direction.SHORT
                position.volume *= -1
            self.gateway.on_position(position)

        self.gateway.write_log("持仓更新成功")

    def on_query_deals(self, packet: dict, request: Request) -> None:
        """成交订单查询回报"""
        if not packet["data"]:
            return

        for order_info in packet["data"]:
            direction = Direction.LONG
            if order_info["cmd"] == 1:
                direction = Direction.SHORT

            offset = Offset.OPEN
            if order_info["entry"] == 1:
                offset = Offset.CLOSE

            if order_info["comment"]:
                orderid: str = order_info["comment"]
            else:
                orderid: str = order_info["position_id"]

            trade: TradeData = TradeData(
                symbol=order_info["symbol"],
                exchange=Exchange.HX,
                orderid=orderid,
                tradeid=order_info["deal_id"],
                direction=direction,
                offset=offset,
                price=order_info["price"],
                volume=order_info["volume"] / 10000.0,
                datetime=parse_time(order_info["time"]),
                gateway_name=self.gateway_name,
            )

            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def on_send_order(self, packet: dict, request: Request) -> None:
        """开仓回报"""
        if packet["code"] == 0:
            self.gateway.write_log(f"开仓委托失败：{packet['msg']}")
            return

        elif packet["code"] == 1:
            order, trade = self._parse_trade_data(packet["data"], "open", self.gateway_name)
            self.gateway.on_order(order)
            self.gateway.on_trade(trade)
            self.on_update_position(trade)

            self.query_account()
            self.gateway.write_log("订单委托成功")

    def on_cancel_order(self, packet: dict, request: Request) -> None:
        """平仓回报"""
        if packet["code"] == 0:
            self.gateway.write_log(f"平仓委托失败：{packet['msg']}")
            return

        elif packet["code"] == 1:
            for data in packet["data"]:
                order, trade = self._parse_trade_data(data, "close", self.gateway_name)
                self.gateway.on_order(order)
                self.gateway.on_trade(trade)
                self.on_update_position(trade)

            self.query_account()
            self.gateway.write_log("平仓委托成功")

    def on_modify_order(self, packet: dict, request: Request) -> None:
        """订单修改回报"""
        if packet["code"] == 1:
            self.query_order()
        self.gateway.write_log("修改订单成功")

    def on_error(
            self,
            exception_type: type,
            exception_value: Exception,
            tb: TracebackType,
            request: Request
    ) -> None:
        """触发异常回报"""
        msg: str = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        buf: Dict[datetime, BarData] = {}
        path: str = "/v2/candles"
        # 创建查询参数
        params: dict = {
            "symbol": req.symbol,
            "bar": INTERVAL_VT2HX[req.interval]
        }
        # 从服务器获取响应
        resp: Response = self.request(
            "POST",
            path,
            data=params
        )

        if resp.status_code != 200:
            msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.gateway.write_log(msg)
        else:
            data: dict = resp.json()
            if data['code'] == 1 and data["data"]:
                for bar_list in data["data"]:
                    # id, open, high, low, close, count = bar_list
                    dt = parse_timestamp(bar_list['id'])
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=bar_list['count'],
                        open_price=bar_list['open'],
                        high_price=bar_list['high'],
                        low_price=bar_list['low'],
                        close_price=bar_list['close'],
                        gateway_name=self.gateway_name
                    )
                    buf[bar.datetime] = bar
                msg: str = "获取历史数据成功"
                self.gateway.write_log(msg)

        index: List[datetime] = list(buf.keys())
        index.sort()

        history: List[BarData] = [buf[i] for i in index]
        return history

    def _new_order_id(self) -> int:
        """生成委托号"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def _parse_order_data(self, data: dict, gateway_name: str) -> OrderData:
        """解析委托回报数据"""
        comment: str = str(data["comment"])
        ticket: str = str(data["ticket"])
        if comment:
            order_id: str = comment
            self.comment_ticket_map[order_id] = ticket
        else:
            order_id: str = ticket

        direction = Direction.SHORT
        types = OrderType.MARKET
        status = Status.NOTTRADED
        if data["cmd"] % 2 == 0:
            direction = Direction.LONG
        if data["cmd"] > 1:
            types = OrderType.LIMIT

        order = OrderData(
            exchange=Exchange.HX,
            symbol=data["symbol"],
            orderid=order_id,
            price=data["open_price"],
            volume=data["volume"],
            datetime=parse_time(data["open_time"]),
            type=types,
            direction=direction,
            offset=Offset.OPEN,
            status=status,
            traded=0,
            gateway_name=gateway_name,
        )
        return order

    def _parse_trade_data(self, data: dict, oc: str, gateway_name: str) -> Tuple[OrderData, TradeData]:
        """解析开/平仓回报数据"""
        # 绑定comment与ticket
        comment: str = str(data["comment"])
        ticket: str = str(data["ticket"])
        if comment:
            order_id: str = comment
            self.comment_ticket_map[order_id] = ticket
        else:
            order_id: str = ticket

        # 解析委托
        types = OrderType.MARKET
        if data["cmd"] > 1:
            types = OrderType.LIMIT

        if oc == "open":
            offset = Offset.OPEN
            status = Status.NOTTRADED
            if data["cmd"] % 2 == 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

        elif oc == "close":
            offset = Offset.CLOSE
            status = Status.ALLTRADED
            if data["cmd"] % 2 == 0:
                direction = Direction.SHORT
            else:
                direction = Direction.LONG

        order: OrderData = OrderData(
            exchange=Exchange.HX,
            symbol=data["symbol"],
            orderid=order_id,
            price=data["price"],
            volume=data["volume"],
            datetime=parse_timestamp(data["time"]),
            type=types,
            direction=direction,
            offset=offset,
            status=status,
            traded=0,
            gateway_name=gateway_name,
        )

        trade: TradeData = TradeData(
            symbol=data["symbol"],
            exchange=Exchange.HX,
            orderid=order_id,
            tradeid=data["deal_id"],
            direction=direction,
            offset=offset,
            price=data["price"],
            volume=data["volume"],
            datetime=parse_timestamp(data["time"]),
            gateway_name=gateway_name,
        )

        return order, trade


class HxWebsocketApi(WebsocketClient):
    """火象websocket接口"""

    def __init__(self, gateway: HxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: HxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.callbacks: Dict[str, callable] = {
            "symbol": self.on_instrument,
            "quote": self.on_tick,
        }
        self.key: str = ""
        self.secret: str = ""

    def connect(
        self,
        key: str,
        secret: str
    ) -> None:
        """连接Websocket私有频道"""
        self.key = key
        self.secret = secret
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        self.init(PUBLIC_WEBSOCKET_HOST, "", 0, 20)
        self.start()

    def query_contract(self) -> None:
        """查询合约信息"""
        # 发送查询请求
        req: dict = {
            "cmd": "symbols",
            "args": [""]
        }
        self.send_packet(req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅合约行情"""
        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        self.subscribed[req.vt_symbol] = req

        # 订阅合约
        symbols = []
        for v in self.subscribed.values():
            symbols.append(v.symbol)

        req: dict = {
            "cmd": "quote",
            "args": [','.join(symbols)]
        }
        self.send_packet(req)

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("Websocket Private API连接成功")
        self.query_contract()
        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("Websocket Private API连接断开")

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if "event" in packet:
            cb_name: str = packet["event"]
        elif "type" in packet:
            cb_name: str = packet["type"]

        callback: callable = self.callbacks.get(cb_name, None)
        if callback:
            callback(packet)

    def on_tick(self, data) -> None:
        """tick数据回报"""
        if "data" not in data:
            return

        d: dict = data["data"]
        tick: TickData = TickData(
            gateway_name=self.gateway_name,
            symbol=d["symbol"],
            exchange=Exchange.HX,
            datetime=parse_timestamp(d["id"]),

            name="",
            last_price=float(d["ask"]),
            bid_price_1=float(d["bid"]),
            ask_price_1=float(d["ask"]),
            bid_volume_1=0,
            ask_volume_1=0
        )

        if tick.ask_price_1 and tick.bid_price_1 and tick.datetime:
            self.gateway.on_tick(copy(tick))

    def on_instrument(self, data: list) -> None:
        """合约查询回报"""
        for d in data["data"]:
            symbol: str = d["name"]
            net_position: bool = True
            size: float = d["size"]
            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.HX,
                name=symbol,
                product=PRODUCTTYPE_HX2VT.get(d["group"], Product.FUTURES),
                size=size,
                pricetick=d['point'],
                min_volume=0.01,
                history_data=True,
                net_position=net_position,
                gateway_name=self.gateway_name,
            )
            symbol_contract_map[contract.symbol] = contract

            self.gateway.on_contract(contract)
        self.gateway.write_log("合约信息查询成功")

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """触发异常回报"""
        msg: str = f"私有频道触发异常，类型：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb)
        )

    def on_api_error(self, packet: dict) -> None:
        """用户登录请求回报"""
        code: str = packet["code"]
        msg: str = packet["msg"]
        self.gateway.write_log(f"Websocket Private API请求失败, 状态码：{code}, 信息：{msg}")


def generate_signature(msg: str, secret_key: str) -> bytes:
    """生成签名"""
    # return base64.b64encode(hmac.new(secret_key.encode(), msg.encode(), hashlib.sha256).digest())
    return hmac.new(secret_key.encode(), msg.encode(), digestmod=hashlib.sha256).hexdigest()


def generate_time():
    return int(time.time())


def generate_timestamp() -> str:
    """生成时间戳"""
    return str(int(time.time()))


def parse_timestamp(timestamp: str) -> datetime:
    """解析回报时间戳"""
    dt: datetime = datetime.fromtimestamp(int(timestamp), CHINA_TZ)
    return dt


def parse_time(string: str) -> datetime:
    """解析回报时间"""
    dt: datetime = datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    return CHINA_TZ.localize(dt)


def get_float_value(data: dict, key: str) -> float:
    """获取字典中对应键的浮点数值"""
    data_str = data.get(key, "")
    if not data_str:
        return 0.0
    return float(data_str)
