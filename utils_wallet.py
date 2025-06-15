import logging
import base58
import os
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from dotenv import load_dotenv

load_dotenv()

async def get_sol_balance(wallet_address: str) -> dict:
    """
    查詢 SOL 錢包餘額，回傳 {'decimals': 9, 'balance': {'int': 0, 'float': 0.0}, 'lamports': 0}
    """
    primary_rpc = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    fallback_rpcs = [
        "https://solana-api.projectserum.com",
        "https://rpc.ankr.com/solana",
        "https://solana-mainnet.rpc.extrnode.com"
    ]
    rpcs_to_try = [primary_rpc] + fallback_rpcs
    max_retries = 3
    default_balance = {"decimals": 9, "balance": {"int": 0, "float": 0.0}, "lamports": 0}
    try:
        pubkey = Pubkey(base58.b58decode(wallet_address))
    except Exception as e:
        logging.error(f"轉換錢包地址失敗: {e}")
        return default_balance
    for retry in range(max_retries):
        for rpc_url in rpcs_to_try:
            client = None
            try:
                client = AsyncClient(rpc_url, timeout=15)
                balance_response = await client.get_balance(pubkey=pubkey)
                lamports = balance_response.value
                sol_balance = lamports / 10**9
                # 這裡可加 SOL 價格查詢
                return {
                    'decimals': 9,
                    'balance': {'int': sol_balance, 'float': sol_balance},
                    'lamports': lamports
                }
            except Exception as e:
                logging.error(f"RPC {rpc_url} 查詢失敗: {e}")
            finally:
                if client:
                    try:
                        await client.close()
                    except:
                        pass
    return default_balance 