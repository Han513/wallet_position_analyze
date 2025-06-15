#!/usr/bin/env python3
"""
交易型態判斷邏輯測試腳本
"""

import sys
import os

# 添加當前目錄到路徑，以便導入本地模組
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def determine_transaction_type(side, amount, current_holding):
    """
    判斷交易型態 (從主程式複製的函數)
    
    Args:
        side: 'buy' 或 'sell'
        amount: 交易數量
        current_holding: 當前持倉數量
    
    Returns:
        int: 0=清倉, 1=建倉, 2=加倉, 3=減倉
    """
    if side == 'buy':
        if current_holding <= 1e-9:  # 基本上沒有持倉 (考慮浮點精度)
            return 1  # 建倉
        else:
            return 2  # 加倉
    elif side == 'sell':
        if current_holding <= 1e-9:  # 沒有持倉卻在賣出 (理論上不應該發生，但防禦性處理)
            return 3  # 減倉
        elif amount >= current_holding - 1e-9:  # 賣出數量 >= 持倉數量 (考慮浮點精度)
            return 0  # 清倉
        else:
            return 3  # 減倉
    else:
        # 未知交易類型，默認返回1
        return 1

def get_type_name(type_code):
    """返回交易型態的中文名稱"""
    names = {
        0: '清倉',
        1: '建倉', 
        2: '加倉',
        3: '減倉'
    }
    return names.get(type_code, '未知')

def test_transaction_scenarios():
    """測試各種交易場景"""
    
    print("=== 交易型態判斷邏輯測試 ===\n")
    
    test_cases = [
        # (side, amount, current_holding, expected_type, description)
        ('buy', 100, 0, 1, '無持倉時買入 - 應為建倉'),
        ('buy', 100, 1e-10, 1, '微量持倉時買入 - 應為建倉(浮點精度)'),
        ('buy', 50, 100, 2, '有持倉時買入 - 應為加倉'),
        ('buy', 200, 500, 2, '大量加倉 - 應為加倉'),
        
        ('sell', 100, 100, 0, '賣出全部持倉 - 應為清倉'),
        ('sell', 100, 100.0000001, 0, '賣出全部持倉(浮點精度) - 應為清倉'),
        ('sell', 50, 100, 3, '部分賣出 - 應為減倉'),
        ('sell', 25, 100, 3, '小量減倉 - 應為減倉'),
        ('sell', 101, 100, 0, '賣出超過持倉 - 應為清倉'),
        
        ('sell', 50, 0, 3, '無持倉時賣出(異常情況) - 應為減倉'),
        ('sell', 50, 1e-10, 3, '微量持倉時賣出(異常情況) - 應為減倉'),
        
        ('unknown', 100, 50, 1, '未知交易類型 - 應為建倉(默認)'),
    ]
    
    success_count = 0
    total_count = len(test_cases)
    
    for i, (side, amount, holding, expected, description) in enumerate(test_cases, 1):
        result = determine_transaction_type(side, amount, holding)
        type_name = get_type_name(result)
        expected_name = get_type_name(expected)
        
        status = "✓" if result == expected else "✗"
        
        print(f"測試 {i:2d}: {status} {description}")
        print(f"        參數: side={side}, amount={amount}, holding={holding}")
        print(f"        結果: {result}({type_name}) | 預期: {expected}({expected_name})")
        
        if result == expected:
            success_count += 1
        else:
            print(f"        ❌ 測試失敗!")
        
        print()
    
    print(f"=== 測試結果總結 ===")
    print(f"通過: {success_count}/{total_count}")
    print(f"成功率: {success_count/total_count*100:.1f}%")
    
    if success_count == total_count:
        print("🎉 所有測試通過!")
        return True
    else:
        print("❌ 部分測試失敗，請檢查邏輯!")
        return False

def test_realistic_trading_sequence():
    """測試真實的交易序列"""
    
    print("\n=== 真實交易序列測試 ===\n")
    
    # 模擬一個完整的交易生命週期
    trades = [
        ('buy', 1000, '首次建倉'),
        ('buy', 500, '看好加倉'), 
        ('sell', 300, '部分獲利'),
        ('buy', 200, '下跌補倉'),
        ('sell', 1400, '全部清倉'),
        ('buy', 800, '重新建倉'),
        ('sell', 400, '減倉一半'),
        ('sell', 400, '清倉剩餘'),
    ]
    
    current_holding = 0
    
    print("交易序列模擬:")
    print("時間  | 操作   | 數量  | 持倉前 | 型態 | 型態名 | 持倉後 | 描述")
    print("-" * 70)
    
    for i, (side, amount, desc) in enumerate(trades, 1):
        holding_before = current_holding
        tx_type = determine_transaction_type(side, amount, current_holding)
        type_name = get_type_name(tx_type)
        
        # 更新持倉
        if side == 'buy':
            current_holding += amount
        elif side == 'sell':
            current_holding = max(0, current_holding - amount)
        
        print(f"{i:4d}  | {side:4s} | {amount:5d} | {holding_before:6.0f} | {tx_type:2d}   | {type_name:2s} | {current_holding:6.0f} | {desc}")
    
    print("\n說明:")
    print("0=清倉, 1=建倉, 2=加倉, 3=減倉")

def test_edge_cases():
    """測試邊界情況"""
    
    print("\n=== 邊界情況測試 ===\n")
    
    edge_cases = [
        ('buy', 0, 0, '零數量買入'),
        ('sell', 0, 100, '零數量賣出'),
        ('buy', 1e-15, 0, '極小數量買入'),
        ('sell', 1e-15, 1e-14, '極小數量賣出'),
        ('buy', 1e10, 1e10, '極大數量交易'),
        ('sell', 100.0000001, 100, '浮點精度測試'),
    ]
    
    for side, amount, holding, desc in edge_cases:
        tx_type = determine_transaction_type(side, amount, holding)
        type_name = get_type_name(tx_type)
        print(f"{desc}: side={side}, amount={amount}, holding={holding}")
        print(f"  結果: {tx_type}({type_name})\n")

def main():
    """主測試函數"""
    success = test_transaction_scenarios()
    test_realistic_trading_sequence()
    test_edge_cases()
    
    print("\n=== 測試完成 ===")
    if success:
        print("✅ 邏輯驗證通過，可以部署到生產環境")
        return 0
    else:
        print("❌ 發現問題，請修復後重新測試")
        return 1

if __name__ == '__main__':
    exit(main()) 