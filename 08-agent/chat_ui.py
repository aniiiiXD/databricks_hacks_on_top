"""
Digital-Artha Chat UI (Gradio)
Run: python3 chat_ui.py
Opens at: http://localhost:7860
"""

import gradio as gr
import requests
import json

AGENT_URL = "http://localhost:8000/invocations"

def chat(message, history):
    try:
        messages = []
        for h in history:
            if isinstance(h, dict):
                messages.append(h)
            elif isinstance(h, (list, tuple)) and len(h) == 2:
                messages.append({"role": "user", "content": h[0]})
                if h[1]:
                    messages.append({"role": "assistant", "content": h[1]})
        messages.append({"role": "user", "content": message})

        response = requests.post(
            AGENT_URL,
            json={"input": messages},
            headers={"Content-Type": "application/json"},
            timeout=120
        )

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                if "output" in data:
                    output = data["output"]
                    if isinstance(output, list):
                        texts = [item.get("text", "") for item in output if isinstance(item, dict) and item.get("type") == "message"]
                        return "\n".join(texts) if texts else json.dumps(output, indent=2)
                    return str(output)
                elif "choices" in data:
                    return data["choices"][0]["message"]["content"]
                return json.dumps(data, indent=2)
            return str(data)
        else:
            return f"Error {response.status_code}: {response.text[:500]}"
    except requests.exceptions.ConnectionError:
        return "Cannot connect to agent. Make sure `uv run start-server` is running on port 8000."
    except Exception as e:
        return f"Error: {str(e)}"


demo = gr.ChatInterface(
    fn=chat,
    title="Digital-Artha: Financial Intelligence for India",
    description="**UPI Fraud Detection** | **RBI Regulations** | **Loan Scheme Finder**\n\nTry: 'Show me fraud alerts' or 'What schemes for a farmer in UP?'",
    examples=[
        "Show me the top 5 highest risk fraud alerts",
        "What should I do if I was scammed via QR code?",
        "I am a 30 year old street vendor from Maharashtra earning 1.5 lakh. What schemes can I apply for?",
        "What are the RBI guidelines for UPI fraud prevention?",
        "Which merchant categories have the highest fraud rate?",
    ],
)

if __name__ == "__main__":
    demo.launch(server_port=7860)
