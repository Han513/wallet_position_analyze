from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, BIGINT, UniqueConstraint

Base = declarative_base()

def get_utc8_time():
    from datetime import datetime, timedelta, timezone
    return datetime.now(timezone(timedelta(hours=8)))

class Transaction(Base):
    """交易記錄表，用於存儲所有交易"""
    __tablename__ = 'wallet_transaction'
    __table_args__ = (
        UniqueConstraint('signature', 'wallet_address', 'token_address', 'transaction_time', name='wallet_transaction_pkey'),
        {'schema': 'dex_query_v1'}
    )

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="聰明錢錢包地址")
    wallet_balance = Column(Float, nullable=False, comment="聰明錢錢包餘額(SOL+U)")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    token_icon = Column(Text, nullable=True, comment="代幣圖片網址")
    token_name = Column(Text, nullable=True, comment="代幣名稱")
    price = Column(Float, nullable=True, comment="價格")
    amount = Column(Float, nullable=False, comment="數量")
    marketcap = Column(Float, nullable=True, comment="市值")
    value = Column(Float, nullable=True, comment="價值")
    holding_percentage = Column(Float, nullable=True, comment="倉位百分比")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    chain_id = Column(Integer, nullable=False, comment="區塊鏈ID")
    realized_profit = Column(Float, nullable=True, comment="已實現利潤")
    realized_profit_percentage = Column(Float, nullable=True, comment="已實現利潤百分比")
    transaction_type = Column(String(10), nullable=False, comment="事件 (buy, sell)")
    transaction_time = Column(BIGINT, nullable=False, comment="交易時間")
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    signature = Column(Text, nullable=False, comment="交易簽名")
    from_token_address = Column(String(100), nullable=False, comment="from token 地址")
    from_token_symbol = Column(Text, nullable=True, comment="from token 名稱")
    from_token_amount = Column(Float, nullable=False, comment="from token 數量")
    dest_token_address = Column(String(100), nullable=False, comment="dest token 地址")
    dest_token_symbol = Column(Text, nullable=True, comment="dest token 名稱")
    dest_token_amount = Column(Float, nullable=False, comment="dest token 數量")

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
