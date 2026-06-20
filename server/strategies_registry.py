"""向后兼容 re-export —— 实际定义已搬到 quant.strategies.registry。

新代码应该从 quant.strategies 直接 import：
    from quant.strategies import build_strategy, list_strategies, STRATEGIES
"""
from quant.strategies.registry import STRATEGIES, build_strategy, list_strategies

__all__ = ["STRATEGIES", "build_strategy", "list_strategies"]
