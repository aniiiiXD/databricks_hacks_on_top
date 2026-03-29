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
        # Build input in MLflow ResponsesAgent format
        input_messages = []
        for h in history:
            if isinstance(h, dict):
                input_messages.append(h)
            elif isinstance(h, (list, tuple)) and len(h) == 2:
                input_messages.append({
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": h[0]}]
                })
                if h[1]:
                    input_messages.append({
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": h[1]}]
                    })

        # Add current message
        input_messages.append({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": message}]
        })

        payload = {"input": input_messages}

        response = requests.post(
            AGENT_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "output" in data:
                output = data["output"]
                if isinstance(output, list):
                    texts = []
                    for item in output:
                        if isinstance(item, dict):
                            content = item.get("content", [])
                            if isinstance(content, list):
                                for c in content:
                                    if isinstance(c, dict) and "text" in c:
                                        texts.append(c["text"])
                            elif isinstance(content, str):
                                texts.append(content)
                    return "\n".join(texts) if texts else json.dumps(output, indent=2, default=str)
                return str(output)
            return json.dumps(data, indent=2, default=str)
        else:
            return f"Error {response.status_code}: {response.text[:500]}"

    except requests.exceptions.ConnectionError:
        return "Cannot connect to agent. Make sure `uv run start-server` is running on port 8000."
    except Exception as e:
        return f"Error: {str(e)}"


demo = gr.ChatInterface(
    fn=chat,
    title="Digital-Artha: Financial Intelligence for India",
    description="**UPI Fraud Detection** | **RBI Regulations** | **Loan Scheme Finder**\n\nTry the examples below or ask anything about UPI fraud, RBI rules, or government schemes.",
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
