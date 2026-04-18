"""Incentive title validation helpers."""

from __future__ import annotations


def classify_incentive_status(saldo: float, missing_title: bool = False) -> tuple[str, str]:
    """Classify incentive receivable status from balance and lookup state."""
    if missing_title:
        return "VERIFICAR", "Há título de incentivo não encontrado em bdnContasReceber"
    if abs(saldo) < 0.01:
        return "APTO", "Todos os títulos de incentivo estão quitados"
    return "NÃO APTO", "Há saldo pendente em título(s) de incentivo"

