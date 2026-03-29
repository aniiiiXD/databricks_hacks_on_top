"""
Digital-Artha Chat UI (Gradio)
Run: pip install gradio requests && python chat_ui.py
Opens at: http://localhost:7860
"""

import gradio as gr
import requests
import json

AGENT_URL = "http://localhost:8000/invocations"

def chat(message, history):
    """Send message to agent and get response."""
    try:
        # Build messages from history + new message
        messages = []
        for h in history:
            messages.append({"role": "user", "content": h[0]})
            if h[1]:
                messages.append({"role": "assistant", "content": h[1]})
        messages.append({"role": "user", "content": message})

        # Try Responses API format
        payload = {
            "input": messages
        }

        response = requests.post(
            AGENT_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )

        if response.status_code == 200:
            data = response.json()
            # Extract text from response
            if isinstance(data, dict):
                # ResponsesAgent format
                if "output" in data:
                    output = data["output"]
                    if isinstance(output, list):
                        texts = [item.get("text", "") for item in output if item.get("type") == "message"]
                        return "\n".join(texts) if texts else str(output)
                    return str(output)
                elif "choices" in data:
                    return data["choices"][0]["message"]["content"]
                elif "content" in data:
                    return data["content"]
                else:
                    return json.dumps(data, indent=2)
            return str(data)
        else:
            return f"Error {response.status_code}: {response.text[:500]}"

    except requests.exceptions.ConnectionError:
        return "Cannot connect to agent. Make sure `uv run start-app` is running on port 8000."
    except Exception as e:
        return f"Error: {str(e)}"


# Build Gradio UI
with gr.Blocks(
    title="Digital-Artha: Financial Intelligence",
    theme=gr.themes.Soft(primary_hue="blue"),
) as demo:
    gr.Markdown("""
    # 🏦 Digital-Artha: Financial Intelligence for India
    **UPI Fraud Detection** | **RBI Regulations** | **Loan Scheme Finder**

    Try asking:
    - "Show me recent fraud alerts"
    - "What are RBI guidelines on UPI fraud prevention?"
    - "I am a 25 year old farmer from UP earning 2 lakh"
    - "यूपीआई फ्रॉड के बारे में बताओ" (Hindi)
    """)

    chatbot = gr.ChatInterface(
        fn=chat,
        examples=[
            "Show me the top 5 highest risk fraud alerts",
            "What should I do if I was scammed via QR code?",
            "I am a 30 year old street vendor from Maharashtra earning 1.5 lakh per year. What government schemes can I apply for?",
            "What are the RBI guidelines for digital payment fraud prevention?",
            "Which merchant categories have the highest fraud rate?",
        ],
        title="",
        retry_btn="Retry",
        undo_btn="Undo",
        clear_btn="Clear",
    )

    gr.Markdown("---\n*Powered by Databricks + LangGraph + FAISS + Foundation Model API*")

if __name__ == "__main__":
    demo.launch(server_port=7860, share=False)
