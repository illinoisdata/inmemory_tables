#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2021-2022 University of Illinois
from typing import List

from core.algorithm.optimize_order.order_optimizer import OrderOptimizer


class OptimizeOrderNone(OrderOptimizer):
    """
        Don't do anything and return the current execution order.

        Args:
            debug (bool):
                whether to print debug messages during optimization.
    """
    def __init__(self, debug=False):
        super().__init__(debug)

    def optimize_order(self) -> List[str]:
        return self.cur_execution_order

