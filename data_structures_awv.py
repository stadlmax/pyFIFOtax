import datetime
import decimal
import pandas as pd
from datetime import datetime

from utils import to_decimal, round_decimal, get_daily_rate


class AWVEntry:
    def __init__(self):
        # modify when regulations change
        self.awv_threshold_eur = to_decimal(12_500)

    def set_threshold(self, val: int):
        self.awv_threshold_eur = to_decimal(val)

    def as_dict(self):
        raise NotImplementedError

    def apply_daily_rate(self, daily_rates: pd.DataFrame):
        self.value_eur = self.value / get_daily_rate(
            daily_rates, self.date, self.currency
        )


class AWVEntryZ4(AWVEntry):
    def __init__(
        self,
        date: datetime,
        purpose: str,
        value: decimal,
        is_incoming: bool,
    ):
        super().__init__()
        self.date = date
        self.purpose = purpose
        self.currency = "USD"
        self.value = value
        self.value_eur = None
        self.is_incoming = is_incoming

    def as_dict(self):
        assert self.value_eur is not None
        if self.value_eur < self.awv_threshold_eur:
            return None

        value_eur = str(
            round_decimal(self.value_eur / to_decimal(1_000), precision="1")
        )

        tmp = {
            "Meldezeitraum": f"{self.date.year}-{self.date.month}",
            "Zweck der Zahlung": self.purpose,
            "BA": 1,
            "Kennzahl": 521,
            "Land": "USA",
            "Land-Code": "US",
            "Eingehende Zahlungen": value_eur if self.is_incoming else "",
            "Ausgehende Zahlungen": value_eur if not self.is_incoming else "",
        }

        return tmp


class AWVEntryZ10(AWVEntry):
    def __init__(
        self,
        date: datetime,
        comment: str,
        quantity: decimal,
        value: decimal,
        is_incoming: bool,
    ):
        super().__init__()
        self.date = date
        self.comment = comment
        self.quantity = quantity
        self.value = value
        self.value_eur = None
        self.currency = "USD"
        self.is_incoming = is_incoming

    def as_dict(self):
        assert self.value_eur is not None
        if self.value_eur < self.awv_threshold_eur:
            return None

        value_eur = str(
            round_decimal(self.value_eur / to_decimal(1_000), precision="1")
        )
        tmp = {
            "Meldezeitraum": f"{self.date.year}-{self.date.month}",
            "Kennzahl": 104,
            "Stückzahl": int(self.quantity),
            "Bezeichnung der Wertpapiere": self.comment,
            "ISIN": "US67066G1040",
            "Land": "USA",
            "Land-Code": "US",
            "Eingehende Zahlungen": value_eur if self.is_incoming else "",
            "Ausgehende Zahlungen": value_eur if not self.is_incoming else "",
        }

        return tmp


class AWVEntryZ10RSUDeposit(AWVEntryZ10):
    def __init__(self, date: datetime, quantity: decimal, value: decimal):
        super().__init__(
            date=date,
            comment="NVIDIA Corp. (Erhalt Aktien aus RSUs)",
            quantity=quantity,
            value=value,
            is_incoming=False,  # buying shares on paper, thus outgoing transaction
        )


class AWVEntryZ10RSUTaxWithholding(AWVEntryZ10):
    def __init__(self, date: datetime, quantity: decimal, value: decimal):
        super().__init__(
            date=date,
            comment="NVIDIA Corp. (Verkauf zur Erzielung dt. EkSt.)",
            quantity=quantity,
            value=value,
            is_incoming=True,  # receiving proceeds to settle tax obligations
        )


class AWVEntryZ10Sale(AWVEntryZ10):
    def __init__(self, date: datetime, quantity: decimal, value: decimal):
        super().__init__(
            date=date,
            comment="NVIDIA Corp. (Verkauf von Aktien aus RSUs/ESPP)",
            quantity=quantity,
            value=value,
            is_incoming=True,  # receiving proceeds of sale
        )


class AWVEntryZ10Buy(AWVEntryZ10):
    def __init__(self, date: datetime, quantity: decimal, value: decimal):
        super().__init__(
            date=date,
            comment="NVIDIA Corp. (Kauf von Aktien)",
            quantity=quantity,
            value=value,
            is_incoming=False,
        )


class AWVEntryZ10ESPPDeposit(AWVEntryZ10):
    def __init__(self, date: datetime, quantity: decimal, value: decimal):
        super().__init__(
            date=date,
            comment="NVIDIA Corp. (Erhalt Aktien aus ESPP)",
            quantity=quantity,
            value=value,
            is_incoming=False,  # buying shares on paper, thus outgoing transaction
        )


class AWVEntryZ4ESPPBonus(AWVEntryZ4):
    def __init__(self, date: datetime, value: decimal):
        super().__init__(
            date=date,
            purpose="Bonuserhalt in Form von Aktien aus ESPPs (NVIDIA Corp.)",
            value=value,
            is_incoming=True,
        )


class AWVEntryZ4RSUBonus(AWVEntryZ4):
    def __init__(self, date: datetime, value: decimal):
        super().__init__(
            date=date,
            purpose="Bonuserhalt in Form von Aktien aus RSUs (NVIDIA Corp.)",
            value=value,
            is_incoming=True,
        )
