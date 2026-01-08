from collections import deque


class SubscriptionManager:
    def __init__(self):
        # 1. 관리자 계정용 (고정 50개) - 예시: 삼성전자, 하이닉스 등 주요 종목
        self.fixed_stocks = set(["005930","000660","207940","005380","000270","055550","105560","068270","015760","028260",
    "032830","012330","035420","006400","086790","006405","000810","010140","064350","138040",
    "051910","010130","009540","267260","066570","066575","033780","003550","003555","310200",
    "034020","012450","009830","011070","071050","081660","046890","323410","017670","010620",
    "047050","009155","275630","009835","001440","138930","175330","051900","092740","034220"])
        # 실제로는 DB나 파일에서 상위 50개를 로드해야 합니다.
        # self._load_top_50_stocks()

        # 2. 사용자 계정용 (관심 20개 + 조회 30개)
        self.interest_stocks = set()  # 최대 20개
        self.viewing_stocks = deque(maxlen=30)  # 조회용 (FIFO), 최대 30개
        self.user_account_stocks = set()  # 실제 웹소켓에 요청할 합집합

    def init_from_api(self, stock_list):
        """API에서 받아온 초기 구독 목록으로 상태 초기화"""
        for code in stock_list:
            if not code: continue

            # 고정 종목은 제외 (Account 1 담당)
            if code in self.fixed_stocks:
                continue

            # Account 2가 담당할 종목들 Viewing 큐에 추가 (최대 30개까지)
            if code not in self.viewing_stocks:
                self.viewing_stocks.append(code)

        # 내부 집합 갱신
        self._refresh_user_account_list()

        # 구독해야 할 리스트 반환
        return list(self.user_account_stocks)

    def update_interest_stocks(self, stock_list):
        """관심 종목 업데이트 (최대 20개 유지 가정)"""
        # 리스트가 들어오면 앞 20개만 취함
        self.interest_stocks = set(stock_list[:20])
        return self._refresh_user_account_list()

    def add_viewing_stock(self, stock_code):
        """사용자가 조회한 종목 추가 (LRU 방식)"""
        if not stock_code:
            return False

        # 1. 고정 종목에 있다면 관리자 계정에서 처리 중이므로 무시
        if stock_code in self.fixed_stocks:
            return False

            # 2. 이미 조회 목록에 있다면 최신으로 갱신 (지웠다 다시 추가)
        if stock_code in self.viewing_stocks:
            self.viewing_stocks.remove(stock_code)

        # 3. 큐에 추가 (꽉 찼다면 가장 오래된 것이 자동으로 밀려남)
        self.viewing_stocks.append(stock_code)

        return self._refresh_user_account_list()

    def _refresh_user_account_list(self):
        """관심 종목 + 조회 종목 합쳐서 사용자 계정 구독 리스트 생성"""
        new_set = self.interest_stocks.union(set(self.viewing_stocks))

        # 변경사항이 있을 때만 True 반환
        if new_set != self.user_account_stocks:
            self.user_account_stocks = new_set
            return True
        return False

    def get_fixed_list(self):
        return list(self.fixed_stocks)

    def get_user_dynamic_list(self):
        return list(self.user_account_stocks)