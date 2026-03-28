#!/usr/bin/env python3
"""Generate UPI fraud recovery guide based on RBI Ombudsman reports and circulars."""
import json

fraud_guide = [
    {
        "fraud_type": "QR Code Scam",
        "description": "Fraudsters replace legitimate merchant QR codes with their own, or send fake QR codes claiming payment is needed to 'receive' money. Victims scan the QR and unknowingly authorize a debit from their account instead of receiving funds.",
        "rbi_liability_rule": "Under RBI's zero-liability framework, if reported within 3 working days and the customer is not negligent, the bank must credit the amount within 10 working days. If the customer shared credentials or was negligent, liability falls on the customer.",
        "recovery_steps": [
            "Step 1: Immediately report the fraud to your bank's customer care and request a transaction reversal or dispute.",
            "Step 2: Lodge a complaint on the NPCI UPI dispute redressal portal (https://www.npci.org.in/what-we-do/upi/dispute-redressal-mechanism).",
            "Step 3: File an FIR at your nearest police station or on the National Cybercrime Portal (https://cybercrime.gov.in).",
            "Step 4: Report to RBI Ombudsman via CMS portal (https://cms.rbi.org.in) if the bank does not resolve within 30 days.",
            "Step 5: Preserve all evidence — screenshots of the QR code, transaction details, and chat history with the fraudster."
        ],
        "report_to": "Bank Customer Care, NPCI, Cybercrime Portal (1930), RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2017-18/15 - DPSS.CO.PD.No.1417/02.14.003/2017-18 (Customer Protection - Limiting Liability)"
    },
    {
        "fraud_type": "Phishing / Vishing Attack",
        "description": "Fraudsters impersonate bank officials, UPI app support, or government agencies via calls, SMS, or emails. They trick victims into revealing UPI PIN, OTP, or clicking malicious links that capture banking credentials.",
        "rbi_liability_rule": "If the customer shares OTP/UPI PIN voluntarily (even under deception), RBI treats this as customer negligence. Liability lies with the customer. However, if the bank's system was compromised, the bank bears full liability regardless of reporting delay.",
        "recovery_steps": [
            "Step 1: Change your UPI PIN immediately on all linked UPI apps.",
            "Step 2: Call your bank's fraud helpline and block UPI transactions temporarily.",
            "Step 3: Report the incident on the National Cybercrime Helpline — call 1930 or visit https://cybercrime.gov.in.",
            "Step 4: File a complaint with your bank in writing and get an acknowledgment with reference number.",
            "Step 5: Escalate to RBI Ombudsman (https://cms.rbi.org.in) if unresolved within 30 days.",
            "Step 6: Alert contacts if your device was compromised to prevent chain-phishing."
        ],
        "report_to": "Bank Fraud Helpline, Cybercrime Helpline (1930), Local Police, RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2017-18/15 - DPSS.CO.PD.No.1417/02.14.003/2017-18 (Customer Protection - Limiting Liability)"
    },
    {
        "fraud_type": "SIM Swap Fraud",
        "description": "Fraudsters obtain a duplicate SIM card of the victim's mobile number by social engineering the telecom provider. Once they control the SIM, they receive OTPs and can reset UPI PINs to drain the linked bank account.",
        "rbi_liability_rule": "If the bank processed transactions after the customer reported SIM swap, the bank is liable. For transactions before reporting, liability depends on whether the breach was from the bank's system or customer negligence. RBI mandates banks to have SIM-change detection mechanisms.",
        "recovery_steps": [
            "Step 1: If you notice sudden loss of mobile network, immediately contact your telecom provider to verify SIM status and block the duplicate SIM.",
            "Step 2: Contact your bank immediately to freeze your account and disable UPI services.",
            "Step 3: File a complaint with the telecom provider's nodal officer in writing.",
            "Step 4: Lodge an FIR at the police station and on https://cybercrime.gov.in.",
            "Step 5: File a complaint with TRAI (Telecom Regulatory Authority of India) if the telecom provider was negligent.",
            "Step 6: Report to RBI Ombudsman and request reversal under zero-liability policy if reported within 3 days."
        ],
        "report_to": "Telecom Provider, Bank, Cybercrime Portal (1930), TRAI, RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2019-20/129 - DPSS.CO.PD.No.116/02.14.006/2019-20 (Online Dispute Resolution for Digital Payments)"
    },
    {
        "fraud_type": "Remote Access / Screen Sharing Fraud",
        "description": "Fraudsters convince victims to install remote access apps (AnyDesk, TeamViewer, QuickSupport) under the guise of tech support or KYC updates. Once installed, they can see the victim's screen, capture UPI PINs, and initiate unauthorized transactions.",
        "rbi_liability_rule": "Since the customer voluntarily installed the remote access app, RBI considers this customer negligence. Full liability rests with the customer. However, banks are expected to send real-time alerts and provide mechanisms for immediate blocking.",
        "recovery_steps": [
            "Step 1: Immediately uninstall the remote access application and restart your device.",
            "Step 2: Change UPI PINs and passwords for all banking apps from a different secure device.",
            "Step 3: Contact your bank to block all pending and future UPI transactions.",
            "Step 4: Report to Cybercrime Helpline (1930) and file an FIR.",
            "Step 5: Run a malware/antivirus scan on your device.",
            "Step 6: Check and revoke any unknown device access in your UPI app settings.",
            "Step 7: Escalate to RBI Ombudsman if bank does not respond within 30 days."
        ],
        "report_to": "Bank Customer Care, Cybercrime Helpline (1930), Local Police, RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2020-21/74 - CO.DPSS.POLC.No.S-516/02-14-003/2020-21 (Security and Risk Mitigation for Digital Payments)"
    },
    {
        "fraud_type": "Fake UPI ID / Impersonation Fraud",
        "description": "Fraudsters create UPI IDs that closely resemble legitimate businesses or known contacts (e.g., 'amazonpay@ybl' vs 'amaz0npay@ybl'). Victims send money to these fake IDs believing they are paying a genuine entity.",
        "rbi_liability_rule": "Since the customer initiates the payment to the wrong UPI ID, this is considered customer negligence. However, PSPs (Payment Service Providers) are required under NPCI guidelines to display beneficiary name before transaction confirmation. If the PSP failed to display the name, partial liability may apply.",
        "recovery_steps": [
            "Step 1: Note the exact UPI ID you paid to and the transaction reference number.",
            "Step 2: Report to your UPI app's grievance mechanism and request a dispute resolution.",
            "Step 3: Contact the beneficiary bank (identified from the UPI handle, e.g., @ybl = Yes Bank) and request a freeze on the recipient account.",
            "Step 4: File an FIR and report on https://cybercrime.gov.in with complete transaction details.",
            "Step 5: File a complaint with NPCI via their dispute redressal system.",
            "Step 6: If the fraudulent account is frozen in time, recovery is possible through police intervention."
        ],
        "report_to": "UPI App Support, Beneficiary Bank, NPCI, Cybercrime Portal (1930), Police",
        "time_limit_days": 3,
        "relevant_circular": "NPCI Circular OC-151 (UPI Operating Procedures - Dispute Resolution and Beneficiary Name Display)"
    },
    {
        "fraud_type": "Collect Request Fraud",
        "description": "Fraudsters send 'collect money' requests via UPI to victims with deceptive messages like 'Cashback of Rs 500' or 'Refund Processing'. Victims, confusing the collect request with incoming money, enter their UPI PIN and end up sending money to the fraudster.",
        "rbi_liability_rule": "Since the customer authorizes the transaction by entering the UPI PIN, RBI considers this customer negligence. However, NPCI requires apps to clearly distinguish between pay and collect requests and display prominent warnings.",
        "recovery_steps": [
            "Step 1: Never enter your UPI PIN to 'receive' money — this is always a red flag.",
            "Step 2: Report the fraudulent collect request to your UPI app immediately.",
            "Step 3: Contact your bank and file a dispute for the transaction.",
            "Step 4: Report on Cybercrime Helpline (1930) and file an FIR.",
            "Step 5: Block the fraudster's UPI ID in your app.",
            "Step 6: Escalate to RBI Ombudsman if the bank doesn't resolve within 30 days."
        ],
        "report_to": "UPI App Grievance, Bank, Cybercrime Helpline (1930), RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2021-22/160 - DPSS.CO.PD.No.629/02.01.014/2021-22 (Framework for Facilitating Small Value Digital Payments in Offline Mode + UPI Safety Guidelines)"
    },
    {
        "fraud_type": "Merchant / Payment Gateway Fraud",
        "description": "Fake e-commerce websites or apps accept UPI payments for products/services they never deliver. Some fraudulent merchants also overcharge or make duplicate deductions. Includes fake investment platforms, loan apps, and gambling sites that use UPI for collection.",
        "rbi_liability_rule": "For unauthorized transactions through a merchant's compromised system, the bank/PSP bears liability. For transactions the customer willingly made to a fraudulent merchant, recovery depends on merchant traceability. RBI mandates PSPs to conduct merchant due diligence under PA/PG guidelines.",
        "recovery_steps": [
            "Step 1: File a chargeback/dispute with your bank within 3 days of the transaction.",
            "Step 2: Report the merchant to the UPI app and request account suspension.",
            "Step 3: File a consumer complaint on the National Consumer Helpline (NCH) — call 1800-11-4000 or visit https://consumerhelpline.gov.in.",
            "Step 4: Report on Cybercrime Portal (https://cybercrime.gov.in) with merchant details, website URL, and transaction proof.",
            "Step 5: If the merchant is on a known platform (Amazon, Flipkart), report through the platform's fraud mechanism.",
            "Step 6: For investment/loan fraud, also report to SEBI/RBI as applicable.",
            "Step 7: Escalate to RBI Ombudsman for payment-related grievances."
        ],
        "report_to": "Bank, UPI App, National Consumer Helpline (1800-11-4000), Cybercrime Portal, SEBI (if investment fraud), RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2020-21/17 - DPSS.CO.PD.No.1810/02.14.008/2019-20 (Guidelines on Regulation of Payment Aggregators and Payment Gateways)"
    },
    {
        "fraud_type": "Refund Fraud / Double Debit Scam",
        "description": "After a legitimate failed transaction, fraudsters posing as customer support call the victim claiming to process a 'refund'. They send a collect request or ask for UPI PIN/OTP to 'verify the refund'. In reality, the victim ends up making another payment. Also includes cases where merchants falsely claim refund was processed.",
        "rbi_liability_rule": "For genuine failed transactions, RBI mandates auto-reversal within T+5 business days under the TAT (Turn Around Time) framework. If the bank fails to reverse within this period, the customer is entitled to Rs 100/day as compensation. For social engineering refund scams, customer liability applies if they shared credentials.",
        "recovery_steps": [
            "Step 1: For failed transactions, do NOT respond to calls/messages claiming to process refunds — auto-reversal is mandatory.",
            "Step 2: Check your bank statement after 5 business days. If the auto-reversal hasn't happened, file a complaint with the bank.",
            "Step 3: If you fell for a refund scam, immediately report to your bank and block further UPI debits.",
            "Step 4: File a complaint on Cybercrime Portal (https://cybercrime.gov.in) and call 1930.",
            "Step 5: Claim compensation of Rs 100/day from the bank if auto-reversal TAT is breached (cite RBI circular).",
            "Step 6: Escalate to RBI Ombudsman with proof of failed transaction and non-reversal."
        ],
        "report_to": "Bank, NPCI Dispute Portal, Cybercrime Helpline (1930), RBI Ombudsman",
        "time_limit_days": 3,
        "relevant_circular": "RBI/2019-20/67 - DPSS.CO.PD.No.629/02.01.014/2019-20 (Turn Around Time for Resolution of Failed Transactions)"
    }
]

out = "/Users/anixd/Documents/self/Databricks/raw_data/fraud_recovery_guide.json"
with open(out, "w") as f:
    json.dump(fraud_guide, f, indent=2, ensure_ascii=False)

print(f"Generated fraud recovery guide with {len(fraud_guide)} fraud types to {out}")
print(f"Fraud types: {', '.join(f['fraud_type'] for f in fraud_guide)}")
print(f"File size: {__import__('os').path.getsize(out)} bytes")
