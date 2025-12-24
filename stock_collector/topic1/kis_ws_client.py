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
        self.ws_url = os.getenv("KIS_WS_URL")
        self.tr_id = os.getenv("KIS_TR_ID", "H0STCNT0")

        if not self.app_key or not self.app_secret:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")

        self.on_tick = on_tick
        self.approval_key = None
        self.ws = None
        self.connected = False
        self.subscribed = set()

        self.rest_base_url = "https://openapi.koreainvestment.com:9443"

    # -------------------------------
    # Approval Key
    # -------------------------------
    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }

        res = requests.post(url, json=payload)
        res.raise_for_status()

        self.approval_key = res.json()["approval_key"]
        print("🔑 Approval Key issued")

    # -------------------------------
    # WebSocket
    # -------------------------------
    def connect(self):
        if not self.approval_key:
            self.issue_approval_key()

        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_close=self._on_close,
                on_error=self._on_error,
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
        print("⚠️ WS closed, reconnecting...")
        time.sleep(2)
        self.connect()

    def _on_error(self, ws, error):
        print("WS error:", error)

    # -------------------------------
    # Message
    # -------------------------------
    def _on_message(self, ws, message):
        if message.startswith("{"):
            return

        parts = message.split("|")
        if len(parts) < 4 or parts[1] != self.tr_id:
            return

        fields = parts[3].split("^")
        symbol = fields[0]

        if symbol not in self.subscribed:
            return

        try:
            tick = {
                # 기본 식별 / 시간
                "stckShrnIscd": fields[0],          # 종목코드
                "stckCntgHour": fields[1],          # 체결시각 (HHMMSS)

                # 가격 정보
                "stckPrpr": int(float(fields[2])),  # 현재가
                "prdyVrss": float(fields[4]),       # 전일대비
                "prdyCtrt": float(fields[5]),       # 등락률

                # 누적 거래
                "acmlVol": int(float(fields[9])),   # 누적거래량
                "acmlTrPbmn": int(float(fields[10])),  # 누적거래대금

                # 호가 정보
                "askp1": int(float(fields[13])),    # 매도1호가
                "bidp1": int(float(fields[14])),    # 매수1호가

                # 파생 지표
                "wghtAvrgPrc": float(fields[18]),   # 가중평균체결가

                # 체결 수
                "selnCntgCsnu": int(float(fields[21])),  # 매도체결건수
                "shnuCntgCsnu": int(float(fields[22])),  # 매수체결건수

                # 잔량
                "totalAskpRsqn": int(float(fields[23])), # 총매도잔량
                "totalBidpRsqn": int(float(fields[24])), # 총매수잔량
            }

            self.on_tick(tick)

        except Exception as e:
            print("❌ 체결 데이터 파싱 오류:", e)
            print("원본 fields:", fields)


    # -------------------------------
    # Subscribe
    # -------------------------------
    def subscribe(self, symbol: str):
        if symbol in self.subscribed:
            return

        self.subscribed.add(symbol)

        if self.connected:
            self._send_subscribe(symbol)

    def _send_subscribe(self, symbol: str):
        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": self.tr_id,
                    "tr_key": symbol,
                }
            },
        }
        self.ws.send(json.dumps(payload))
