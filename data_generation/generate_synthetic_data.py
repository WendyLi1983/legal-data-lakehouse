import json
import os
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_landing")
os.makedirs(OUT_DIR, exist_ok=True)

COUNTERPARTIES = [
    "BNY Mellon Custody Services", "bny mellon custody services",
    "Northern Trust Fund Services", "Clearstream Banking S.A.",
    "Euroclear Bank", "Moody's Investors Service",
    "Deloitte & Touche LLP", "ISDA Clearing Corp",
]

CONTRACT_TYPES = [
    "ISDA Master Agreement", "Custody Agreement", "Fund Services Agreement",
    "Vendor Risk Agreement", "Data Sharing Agreement", "SLA",
]

CLAUSE_TYPES = [
    "AML/KYC Compliance Review", "SOX Certification",
    "Basel III Capital Reporting", "Regulatory Filing Deadline",
    "Third-Party Risk Assessment", "Data Privacy (GDPR)",
    "Termination", "Confidentiality",
]

STATUSES = ["Open", "open", "Closed", "OVERDUE", "Pending"]

def random_date(start_year=2022, end_year=2026):
    start = datetime((start_year), 1, 1)
    end = datetime((end_year), 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def gen_contracts(num_contracts=300):
    contracts = []
    for i in range(num_contracts):
        effective = random_date(2022,2025)
        contracts.append({
            "contract_id": f"CTR-{1000 + i}",
            "matter_id": f"MTR-{500 + (i % 120)}",
            "counterparty": random.choice(COUNTERPARTIES),
            "contract_type": random.choice(CONTRACT_TYPES),
            "effective_date": effective.strftime("%Y-%m-%d") if i % 3 else effective.strftime("%m/%d/%Y"),
           "expiration_date": (effective + timedelta(days=365 * random.choice([1, 2, 3]))).strftime("%Y-%m-%d"),
           "source_system": random.choice(["LegalTrackerSQL", "OracleContractsDB"]),
            "raw_text_ref": f"s3://legal-docs-raw/contracts/{uuid.uuid4()}.pdf",
           "ingestion_timestamp": datetime.utcnow().isoformat(),
             })
       
 # 注入重复记录,模拟源系统重复推送同一条数据
    contracts += random.sample(contracts, 15)
    return contracts

def gen_obligations(contracts, n=800):
    obligations = []
    contract_ids = [c["contract_id"] for c in contracts]
#   contract_ids = []
#   for c in contracts:
#       contract_ids.append(c["contract_id"])
    for i in range(n):
        due = random_date(2024, 2027)
        obligations.append({
            "obligation_id": f"OBL-{2000 + i}",
            "contract_id": random.choice(contract_ids),
            "clause_type": random.choice(CLAUSE_TYPES),
            "obligation_text": f"Party shall comply with {random.choice(CLAUSE_TYPES).lower()} terms.",
            "due_date": due.strftime("%Y-%m-%d"),
            "status": random.choice(STATUSES),
        })
    # 故意造一条指向不存在合同的孤儿记录,测试referential integrity检查
    obligations.append({
        "obligation_id": "OBL-9999",
        "contract_id": "CTR-DOES-NOT-EXIST",
        "clause_type": "Termination",
        "obligation_text": "Orphaned obligation for testing referential integrity checks.",
        "due_date": "2026-01-01",
        "status": "Open",
    })
    return obligations

if __name__ == "__main__":
    # if __name__ == "__main__": —— 这是Python里一个非常标准的写法,意思是"只有当这个文件
    # 被直接执行的时候(比如python generate_synthetic_data.py),才跑下面这些代码;
    # 如果这个文件被别的脚本import进去当模块用,这部分不会自动执行"。
    # 这是个好习惯,面试被问"为什么要写这个"可以直接讲这个区别。
    contracts = gen_contracts()
    obligations = gen_obligations(contracts)

    with open(os.path.join(OUT_DIR, "contracts_raw.json"), "w") as f:
        for row in contracts:
            f.write(json.dumps(row) + "\n")

    with open(os.path.join(OUT_DIR, "obligations_raw.json"), "w") as f:
        for row in obligations:
            f.write(json.dumps(row) + "\n")

    print(f"Wrote {len(contracts)} contract records and {len(obligations)} obligation records to {OUT_DIR}")