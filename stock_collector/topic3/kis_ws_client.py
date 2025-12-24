import json
import time
import threading
import websocket
import requests
import os
from dotenv import load_dotenv

load_dotenv()

class KISWebSocketClient:
    def __init__(self, on_tick):
        self.app_key = os.getenv("KIS_APP_KEY")
        self.app_secret = os.getenv("KIS_APP_SECRET")

        if not self.app_key or not self.app_secret:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET not found in .env")

        self.on_tick = on_tick

        self.rest_base_url = "https://openapi.koreainvestment.com:9443"
        self.ws_url = "ws://ops.koreainvestment.com:21000/tryitout/websocket"

        self.approval_key = None
        self.ws = None
        self.connected = False

        # 현재 "논리적으로" 구독 중인 종목
        self.subscribed: set[str] = set()

    # ------------------------------------------------------------------
    # Approval Key 발급
    # ------------------------------------------------------------------
    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        headers = {"content-type": "application/json"}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }

        res = requests.post(url, headers=headers, data=json.dumps(payload))
        if res.status_code != 200:
            raise RuntimeError(f"Approval Key 발급 실패: {res.text}")

        self.approval_key = res.json()["approval_key"]
        print("🔑 Approval Key 발급 완료")

    # ------------------------------------------------------------------
    # WebSocket 연결
    # ------------------------------------------------------------------
    def connect(self):
        if not self.approval_key:
            self.issue_approval_key()

        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self.ws.run_forever()

        threading.Thread(target=_run, daemon=True).start()

    def _on_open(self, ws):
        self.connected = True
        print("🔌 KIS WebSocket connected")

        for symbol in self.subscribed:
            self._send_subscribe(symbol)

    def _on_close(self, ws, *args):
        self.connected = False
        print("KIS WebSocket closed, reconnecting in 2s...")
        time.sleep(2)
        self.connect()

    def _on_error(self, ws, error):
        print(" KIS WebSocket error:", error)

    # ------------------------------------------------------------------
    # 안전 변환 유틸 (중요)
    # ------------------------------------------------------------------
    @staticmethod
    def to_int(v):
        try:
            return int(float(v))
        except:
            return 0

    @staticmethod
    def to_float(v):
        try:
            return float(v)
        except:
            return 0.0

    # ------------------------------------------------------------------
    # 메시지 수신 (체결가 파싱 + 구독 필터링)
    # ------------------------------------------------------------------
    def _on_message(self, ws, message):
        if message.startswith("{"):
            return

        parts = message.split("|")
        if len(parts) < 4:
            return

        if parts[1] != "H0STCNT0":
            return

        f = parts[3].split("^")
        symbol = f[0]

        if symbol not in self.subscribed:
            return

        try:
            tick = {
                "stckShrnIscd": f[0],
                "stckCntgHour": f[1],

                "stckPrpr": self.to_int(f[2]),
                "prdyVrss": self.to_float(f[4]),
                "prdyCtrt": self.to_float(f[5]),

                "acmlVol": self.to_int(f[9]),
                "acmlTrPbmn": self.to_int(f[10]),

                "askp1": self.to_int(f[13]),
                "bidp1": self.to_int(f[14]),

                "wghtAvrgPrc": self.to_float(f[18]),

                "selnCntgCsnu": self.to_int(f[21]),
                "shnuCntgCsnu": self.to_int(f[22]),

                "totalAskpRsqn": self.to_int(f[23]),
                "totalBidpRsqn": self.to_int(f[24]),
            }

            self.on_tick(tick)

        except Exception as e:
            print("체결 데이터 파싱 오류:", e)
            print("원본 필드:", f)

    # ------------------------------------------------------------------
    # 4️⃣ 구독 / 해제 (논리적)
    # ------------------------------------------------------------------
    def subscribe(self, symbol):
        if symbol in self.subscribed:
            return

        self.subscribed.add(symbol)

        if self.connected:
            self._send_subscribe(symbol)

        print(f"📡 KIS logical subscribe {symbol}")

    def unsubscribe(self, symbol):
        if symbol not in self.subscribed:
            return

        self.subscribed.remove(symbol)
        print(f"KIS logical unsubscribe {symbol}")

    def _send_subscribe(self, symbol):
        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": symbol,
                }
            },
        }
        self.ws.send(json.dumps(payload))
