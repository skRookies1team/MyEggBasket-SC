from datetime import datetime

class SubscriptionManager:
    def __init__(self):
        self.subscriptions = {}

    def subscribe(self, symbol: str) -> bool:
        """
        return True if this is FIRST subscription (need to start collecting)
        """
        sub = self.subscriptions.get(symbol)

        if not sub:
            self.subscriptions[symbol] = {
                "count": 1,
                "started": True,
                "last_update": datetime.utcnow().isoformat()
            }
            return True  # ▶ start collection

        sub["count"] += 1
        sub["last_update"] = datetime.utcnow().isoformat()
        return False

    def unsubscribe(self, symbol: str) -> bool:
        """
        return True if this is LAST unsubscription (need to stop collecting)
        """
        sub = self.subscriptions.get(symbol)

        if not sub:
            return False

        sub["count"] -= 1
        sub["last_update"] = datetime.utcnow().isoformat()

        if sub["count"] <= 0:
            del self.subscriptions[symbol]
            return True  # ▶ stop collection

        return False

    def is_active(self, symbol: str) -> bool:
        return symbol in self.subscriptions

    def snapshot(self):
        return self.subscriptions.copy()
