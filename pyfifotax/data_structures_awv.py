import decimal
import pandas as pd
import warnings

from datetime import datetime

from pyfifotax.utils import to_decimal, round_decimal, get_daily_rate


class AWVEntry:
    def __init__(self):
        # modify when regulations change
        self.awv_threshold_eur = to_decimal(12_500)
        self.value = None
        self.date = None
        self.currency = None

    def set_threshold(self, val: int):
        self.awv_threshold_eur = to_decimal(val)

    def as_dict(self):
        raise NotImplementedError

    def apply_daily_rate(self, daily_rates: pd.DataFrame):
        if (self.value is None) or (self.date is None) or (self.currency is None):
            raise RuntimeError("calling apply_daily_rate on uninitialized AWVEntry")
        self.value_eur = self.value / get_daily_rate(
            daily_rates,
            self.date,
            self.currency,
            domestic_currency="EUR",
        )


class AWVEntryZ4(AWVEntry):
    def __init__(
        self,
        date: datetime,
        purpose: str,
        value: decimal,
        currency: str,
        is_incoming: bool,
        is_nvidia: bool,
    ):
        super().__init__()
        self.date = date
        self.purpose = purpose
        self.currency = currency
        self.value = value
        self.value_eur = None
        self.is_incoming = is_incoming
        self.is_nvidia = is_nvidia

        if is_nvidia and currency != "USD":
            warnings.warn(
                f"Got currency {currency} for transaction with NVDA, check created reports properly for correctness."
            )

    def as_dict(self):
        if self.value_eur is None:
            raise RuntimeError(
                "Something has gone wrong, currencies haven't been converted to EUR, abort."
            )
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
            "Land": "USA" if self.is_nvidia else "FILL OUT COUNTRY",
            "Land-Code": "US" if self.is_nvidia else "FILL OUT COUNTRY CODE",
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
        currency: str,
        is_incoming: bool,
        is_nvidia: bool,
    ):
        super().__init__()
        self.date = date
        self.comment = comment
        self.quantity = quantity
        self.value = value
        self.value_eur = None
        self.currency = currency
        self.is_incoming = is_incoming
        self.is_nvidia = is_nvidia

        if is_nvidia and currency != "USD":
            warnings.warn(
                f"Got currency {currency} for transaction with NVDA, check created reports properly for correctness."
            )

    def as_dict(self):
        if self.value_eur is None:
            raise RuntimeError(
                "Something has gone wrong, currencies haven't been converted to EUR, abort."
            )
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
            "ISIN": "US67066G1040" if self.is_nvidia else "FILL OUT ISIN",
            "Land": "USA" if self.is_nvidia else "FILL OUT COUNTRY",
            "Land-Code": "US" if self.is_nvidia else "FILL OUT COUNTRY CODE",
            "Eingehende Zahlungen": value_eur if self.is_incoming else "",
            "Ausgehende Zahlungen": value_eur if not self.is_incoming else "",
            "Emissionswährung": (
                self.currency
                if self.is_nvidia
                else f"{self.currency} [VALIDATE CURRENCY]"
            ),
        }

        return tmp


class AWVEntryZ10RSUDeposit(AWVEntryZ10):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        quantity: decimal,
        value: decimal,
        currency: str,
    ):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            comment=(
                "NVIDIA Corp. (Erhalt Aktien aus RSUs)"
                if is_nvidia
                else f"{symbol} [FILL OUT FULL COMPANY NAME] (Erhalt Aktien aus RSUs)"
            ),
            quantity=quantity,
            value=value,
            currency=currency,
            is_nvidia=is_nvidia,
            is_incoming=False,  # buying shares on paper, thus outgoing transaction
        )


class AWVEntryZ10RSUTaxWithholding(AWVEntryZ10):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        quantity: decimal,
        value: decimal,
        currency: str,
    ):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            comment=(
                "NVIDIA Corp. (Verkauf zur Erzielung dt. EkSt.)"
                if is_nvidia
                else f"{symbol} [FILl OUT FULL COMPANY NAME] (Verkauf zur Erzielung dt. EkSt)"
            ),
            quantity=quantity,
            value=value,
            currency=currency,
            is_nvidia=is_nvidia,
            is_incoming=True,  # receiving proceeds to settle tax obligations
        )


class AWVEntryZ10Sale(AWVEntryZ10):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        quantity: decimal,
        value: decimal,
        currency: str,
    ):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            comment=(
                "NVIDIA Corp. (Verkauf von Aktien aus RSUs/ESPP)"
                if is_nvidia
                else f"{symbol} [FILL OUT FULL COMPANY NAME] (Verkauf von Aktien aus RSUs/ESPP)"
            ),
            quantity=quantity,
            value=value,
            is_nvidia=is_nvidia,
            currency=currency,
            is_incoming=True,  # receiving proceeds of sale
        )


class AWVEntryZ10Buy(AWVEntryZ10):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        quantity: decimal,
        value: decimal,
        currency: str,
    ):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            comment=(
                "NVIDIA Corp. (Kauf von Aktien)"
                if is_nvidia
                else f"{symbol} [FILL OUT FULL COMPANY NAME] (Kauf von Aktien)"
            ),
            quantity=quantity,
            value=value,
            is_nvidia=is_nvidia,
            currency=currency,
            is_incoming=False,
        )


class AWVEntryZ10ESPPDeposit(AWVEntryZ10):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        quantity: decimal,
        value: decimal,
        currency: str,
    ):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            comment=(
                "NVIDIA Corp. (Erhalt Aktien aus ESPP)"
                if is_nvidia
                else f"{symbol} [FILL OUT FULL COMPANY NAME] (Erhalt Aktien aus ESPP)"
            ),
            quantity=quantity,
            value=value,
            currency=currency,
            is_nvidia=is_nvidia,
            is_incoming=False,  # buying shares on paper, thus outgoing transaction
        )


class AWVEntryZ4ESPPBonus(AWVEntryZ4):
    def __init__(self, date: datetime, symbol: str, value: decimal, currency: str):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            purpose=(
                "Bonuserhalt in Form von Aktien aus ESPPs (NVIDIA Corp.)"
                if is_nvidia
                else f"Bonuserhalt in Form von Aktien aus ESPPs ({symbol} [FILL OUT FULL COMPANY NAME])"
            ),
            value=value,
            currency=currency,
            is_nvidia=is_nvidia,
            is_incoming=True,
        )


class AWVEntryZ4RSUBonus(AWVEntryZ4):
    def __init__(self, date: datetime, symbol: str, value: decimal, currency: str):
        is_nvidia = "NVDA" in symbol
        super().__init__(
            date=date,
            purpose=(
                "Bonuserhalt in Form von Aktien aus RSUs (NVIDIA Corp.)"
                if is_nvidia
                else f"Bonuserhalt in Form von Aktien aus RSUs ({symbol} [FILL OUT FULL COMPANY NAME])"
            ),
            value=value,
            currency=currency,
            is_nvidia=is_nvidia,
            is_incoming=True,
        )
