import csv
import uuid
import random
import os
import pandas as pd
from datetime import datetime, timedelta

def generate_transactions(num_rings=30):
    transactions = []
    
    # Target Banks for Mules (to display on vulnerability charts)
    VULNERABLE_BANKS = ["Yes Bank", "IndusInd Bank", "Punjab National Bank"]
    NORMAL_BANKS = ["HDFC Bank", "ICICI Bank", "State Bank of India", "Axis Bank", "Kotak Mahindra Bank", "Bank of Baroda"]
    STATES = ["Maharashtra", "Delhi", "Karnataka", "Uttar Pradesh", "Tamil Nadu", "Gujarat", "Andhra Pradesh"]
    
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    for ring_idx in range(num_rings):
        ring_type = random.choice(["star_mule", "triad", "collusion"])
        
        if ring_type == "star_mule":
            # 1 Mule (Vulnerable Bank) receives from 10-20 victims, then sends it to an offshore merchant
            mule_id = f"mule_{uuid.uuid4().hex[:8]}@upi"
            mule_bank = random.choice(VULNERABLE_BANKS)
            mule_state = random.choice(STATES)
            
            # Victims -> Mule
            num_victims = random.randint(15, 30)
            total_amount = 0
            for i in range(num_victims):
                victim_id = f"vic_{uuid.uuid4().hex[:8]}@upi"
                amount = round(random.uniform(500, 25000), 2)
                total_amount += amount
                txn_time = base_time + timedelta(hours=random.randint(0, 72))
                
                transactions.append({
                    "transaction id": f"TXN{uuid.uuid4().hex.upper()}",
                    "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "transaction type": "P2P",
                    "merchant_category": "Personal",
                    "amount (INR)": amount,
                    "transaction_status": "SUCCESS",
                    "sender_age_group": "18-25",
                    "receiver_age_group": "25-40",
                    "sender_state": random.choice(STATES),
                    "sender_bank": random.choice(NORMAL_BANKS),
                    "receiver_bank": mule_bank,
                    "device_type": "Android",
                    "network_type": "4G",
                    "fraud_flag": 1,
                    "hour_of_day": txn_time.hour,
                    "day_of_week": txn_time.strftime("%w"), # numeric day of week
                    "is_weekend": 1 if txn_time.weekday() >= 5 else 0,
                    "explicit_sender_id": victim_id,
                    "explicit_receiver_id": mule_id
                })
                
            # Mule -> Merchant
            merchant_id = f"MERCH_OFFSHORE_{uuid.uuid4().hex[:4]}"
            txn_time = base_time + timedelta(hours=random.randint(73, 96))
            transactions.append({
                "transaction id": f"TXN{uuid.uuid4().hex.upper()}",
                "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                "transaction type": "P2M",
                "merchant_category": "Gaming/Crypto",
                "amount (INR)": total_amount,  # Launder the whole pot
                "transaction_status": "SUCCESS",
                "sender_age_group": "25-40",
                "receiver_age_group": "-",
                "sender_state": mule_state,
                "sender_bank": mule_bank,
                "receiver_bank": "Foreign Bank",
                "device_type": "Android",
                "network_type": "4G",
                "fraud_flag": 1,
                "hour_of_day": txn_time.hour,
                "day_of_week": txn_time.strftime("%w"),
                "is_weekend": 1 if txn_time.weekday() >= 5 else 0,
                "explicit_sender_id": mule_id,
                "explicit_receiver_id": merchant_id
            })
            
        elif ring_type == "triad":
            # A -> B -> C -> A layering
            a_id = f"node_a_{uuid.uuid4().hex[:8]}@upi"
            b_id = f"node_b_{uuid.uuid4().hex[:8]}@upi"
            c_id = f"node_c_{uuid.uuid4().hex[:8]}@upi"
            
            nodes = [a_id, b_id, c_id]
            # Make sure vulnerable bank gets representation in triads
            banks = [random.choice(VULNERABLE_BANKS), random.choice(NORMAL_BANKS), random.choice(VULNERABLE_BANKS)]
            states = [random.choice(STATES) for _ in range(3)]
            
            amount = round(random.uniform(50000, 150000), 2)
            
            for i in range(3):
                sender_idx = i
                recv_idx = (i + 1) % 3
                
                txn_time = base_time + timedelta(minutes=random.randint(5, 60)) * (i+1)
                amount = amount - random.uniform(10, 50)  # Minor fees/attrition
                
                transactions.append({
                    "transaction id": f"TXN{uuid.uuid4().hex.upper()}",
                    "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "transaction type": "P2P",
                    "merchant_category": "Personal",
                    "amount (INR)": round(amount, 2),
                    "transaction_status": "SUCCESS",
                    "sender_age_group": "25-40",
                    "receiver_age_group": "25-40",
                    "sender_state": states[sender_idx],
                    "sender_bank": banks[sender_idx],
                    "receiver_bank": banks[recv_idx],
                    "device_type": "Android",
                    "network_type": "4G",
                    "fraud_flag": 1,
                    "hour_of_day": txn_time.hour,
                    "day_of_week": txn_time.strftime("%w"),
                    "is_weekend": 1 if txn_time.weekday() >= 5 else 0,
                    "explicit_sender_id": nodes[sender_idx],
                    "explicit_receiver_id": nodes[recv_idx]
                })

        elif ring_type == "collusion":
            # Many actors trade with 1 dummy merchant back and forth
            merchant_id = f"MERCH_SHELL_{uuid.uuid4().hex[:4]}"
            merch_bank = random.choice(VULNERABLE_BANKS)
            merch_state = random.choice(STATES)
            
            num_actors = random.randint(5, 12)
            actors = [(f"coll_{uuid.uuid4().hex[:8]}@upi", random.choice(NORMAL_BANKS)) for _ in range(num_actors)]
            
            for _ in range(random.randint(20, 50)):
                actor, actor_bank = random.choice(actors)
                amount = round(random.uniform(5000, 45000), 2)
                txn_time = base_time + timedelta(hours=random.randint(1, 120))
                
                transactions.append({
                    "transaction id": f"TXN{uuid.uuid4().hex.upper()}",
                    "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "transaction type": "P2M",
                    "merchant_category": "Shell Goods",
                    "amount (INR)": amount,
                    "transaction_status": "SUCCESS",
                    "sender_age_group": "30-50",
                    "receiver_age_group": "-",
                    "sender_state": random.choice(STATES),
                    "sender_bank": actor_bank,
                    "receiver_bank": merch_bank,
                    "device_type": "iOS",
                    "network_type": "Wi-Fi",
                    "fraud_flag": 1,
                    "hour_of_day": txn_time.hour,
                    "day_of_week": txn_time.strftime("%w"),
                    "is_weekend": 1 if txn_time.weekday() >= 5 else 0,
                    "explicit_sender_id": actor,
                    "explicit_receiver_id": merchant_id
                })

    return transactions

if __name__ == "__main__":
    txns = generate_transactions(150)  # Generates 150 clusters of heavy fraud rings
    df = pd.DataFrame(txns)
    output_path = os.path.join(os.path.dirname(__file__), "synthetic_fraud_rings.csv")
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} topological transactions spanning 150 rings into {output_path}")

