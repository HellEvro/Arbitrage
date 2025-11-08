 .PHONY: install lint format test run

 install:
 \tpython -m pip install -e ".[dev]"

 lint:
 \truff check .
 \tmypy arbitrage_bot

 format:
 \truff format .

 test:
 \tpytest

 run:
 \tpython main.py

